/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.hardcoded;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.ImmutableCollectors;
import io.airlift.json.JsonCodec;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;

public class HardcodedMetadataPopulator
{
    private final ConnectorManager connectorManager;
    private final JsonCodec<ViewDefinition> viewCodec;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final List<PlanOptimizer> planOptimizers;
    private final boolean experimentalSyntaxEnabled;
    private final AccessControl accessControl;

    public HardcodedMetadataPopulator(ConnectorManager connectorManager, JsonCodec<ViewDefinition> viewCodec, Metadata metadata, SqlParser sqlParser, List<PlanOptimizer> planOptimizers, FeaturesConfig featuresConfig, AccessControl accessControl)
    {
        this.connectorManager = checkNotNull(connectorManager);
        this.viewCodec = viewCodec;
        this.metadata = checkNotNull(metadata);
        this.sqlParser = checkNotNull(sqlParser);
        this.planOptimizers = checkNotNull(planOptimizers);
        checkNotNull(featuresConfig, "featuresConfig is null");
        this.experimentalSyntaxEnabled = featuresConfig.isExperimentalSyntaxEnabled();
        this.accessControl = accessControl;
    }

    public Analysis analyzeStatement(Statement statement, Session session)
    {
        QueryExplainer explainer = new QueryExplainer(planOptimizers, metadata, accessControl, sqlParser, ImmutableMap.of(), experimentalSyntaxEnabled);
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.of(explainer), experimentalSyntaxEnabled);
        return analyzer.analyze(statement);
    }

    @Nullable
    public ViewDefinition buildViewDefinition(Session session, String name, String sql)
    {
        // verify round-trip
        Statement statement;
        try {
            statement = sqlParser.createStatement(sql);
        }
        catch (ParsingException e) {
            throw new PrestoException(INTERNAL_ERROR, "Formatted query does not parse: " + sql);
        }

        Analysis analysis = analyzeStatement(statement, session);

        final List<ViewDefinition.ViewColumn> columns;
        try {
            columns = analysis.getOutputDescriptor()
                    .getVisibleFields().stream()
                    .map(field -> {
                        checkState(field.getName().isPresent(), String.format("view '%s' columns must be named", name));
                        return new ViewDefinition.ViewColumn(field.getName().get(), field.getType());
                    })
                    .collect(toImmutableList());
        }
        catch (SemanticException e) {
            if (e.getCode() == SemanticErrorCode.MISSING_TABLE) {
                return null;
            }
            else {
                throw e;
            }
        }

        return new ViewDefinition(sql, session.getCatalog(), session.getSchema(), columns, Optional.<String>empty());
    }

    private class Context
    {
        public final String name;
        public final HardcodedConnector connector;
        public final HardcodedContents contents;
        public final HardcodedMetadata metadata;

        public Context(String name, HardcodedConnector connector)
        {
            this.name = name;
            this.connector = connector;
            this.contents = connector.getHardcodedContents();
            this.metadata = (HardcodedMetadata) connector.getMetadata();
        }

        public Session createSession(@Nullable String schemaName)
        {
            Session.SessionBuilder builder = Session.builder(new SessionPropertyManager())
                    .setSource("system")
                    .setCatalog(name)
                    .setTimeZoneKey(UTC_KEY)
                    .setLocale(ENGLISH);
            if (schemaName != null) {
                builder.setSchema(schemaName);
            }
            return builder.build();
        }
    }

    public void run()
    {
        List<Context> contexts = connectorManager.getConnectors().entrySet().stream()
                .filter(e -> e.getValue() instanceof HardcodedConnector)
                .map(e -> new Context(e.getKey(), (HardcodedConnector) e.getValue()))
                .collect(ImmutableCollectors.toImmutableList());

        for (Context context : contexts) {
            HardcodedContents contents = context.contents;
            for (Map.Entry<SchemaTableName, String> view : contents.getViews().entrySet()) {
                ViewDefinition viewDefinition = buildViewDefinition(context.createSession(view.getKey().getSchemaName()), view.getKey().toString(), view.getValue());
                if (viewDefinition != null) {
                    String viewJson = viewCodec.toJson(viewDefinition);
                    context.metadata.addView(view.getKey().getSchemaName(), view.getKey().getTableName(), viewJson);
                }
            }
        }
    }
}
