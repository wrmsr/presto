package com.wrmsr.presto.hardcoded;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.ViewDefinition;
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
import com.wrmsr.presto.util.ImmutableCollectors;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Locale.ENGLISH;

public class HardcodedMetadataPopulator
{
    private final ConnectorManager connectorManager;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final List<PlanOptimizer> planOptimizers;
    private final boolean experimentalSyntaxEnabled;

    public HardcodedMetadataPopulator(ConnectorManager connectorManager, Metadata metadata, SqlParser sqlParser, List<PlanOptimizer> planOptimizers, FeaturesConfig featuresConfig)
    {
        this.connectorManager = checkNotNull(connectorManager);
        this.metadata = checkNotNull(metadata);
        this.sqlParser = checkNotNull(sqlParser);
        this.planOptimizers = checkNotNull(planOptimizers);
        checkNotNull(featuresConfig, "featuresConfig is null");
        this.experimentalSyntaxEnabled = featuresConfig.isExperimentalSyntaxEnabled();
    }

    public Analysis analyzeStatement(Statement statement, Session session)
    {
        QueryExplainer explainer = new QueryExplainer(session, planOptimizers, metadata, sqlParser, experimentalSyntaxEnabled);
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, Optional.of(explainer), experimentalSyntaxEnabled);
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

        return new ViewDefinition(sql, session.getCatalog(), session.getSchema(), columns);
    }

    private class Context
    {
        public final String name;
        public final HardcodedConnector connector;
        public final HardcodedContents contents;

        public Context(String name, HardcodedConnector connector)
        {
            this.name = name;
            this.connector = connector;
            this.contents = connector.getHardcodedContents();
        }

        public Session createSession(@Nullable String schemaName)
        {
            Session.SessionBuilder builder = Session.builder()
                    .setUser("system")
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
        System.out.println(contexts);

        for (Context context : contexts) {
            HardcodedContents contents = context.contents;
            for (Map.Entry<SchemaTableName, String> view : contents.getViews().entrySet()) {
                buildViewDefinition(context.createSession(view.getKey().getSchemaName()), view.getKey().toString(), view.getValue());
            }
        }
    }
}
