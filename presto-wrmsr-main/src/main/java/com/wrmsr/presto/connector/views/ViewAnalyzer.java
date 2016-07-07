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
package com.wrmsr.presto.connector.views;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableMap;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class ViewAnalyzer
{
    private final SqlParser sqlParser;
    private final List<PlanOptimizer> planOptimizers;
    private final FeaturesConfig featuresConfig;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final QueryIdGenerator queryIdGenerator;

    @Inject
    public ViewAnalyzer(
            SqlParser sqlParser,
            List<PlanOptimizer> planOptimizers,
            FeaturesConfig featuresConfig,
            Metadata metadata,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            QueryIdGenerator queryIdGenerator)
    {
        this.sqlParser = sqlParser;
        this.planOptimizers = planOptimizers;
        this.featuresConfig = featuresConfig;
        this.metadata = metadata;
        this.accessControl = accessControl;
        this.sessionPropertyManager = sessionPropertyManager;
        this.queryIdGenerator = queryIdGenerator;
    }

    public Analysis analyzeStatement(Statement statement, Session session)
    {
        QueryExplainer explainer = new QueryExplainer(planOptimizers, metadata, accessControl, sqlParser, ImmutableMap.of(), featuresConfig.isExperimentalSyntaxEnabled());
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.of(explainer), featuresConfig.isExperimentalSyntaxEnabled());
        return analyzer.analyze(statement);
    }

    public Session createSession()
    {
        return Session.builder(new SessionPropertyManager())
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(new Identity("user", Optional.empty()))
                .setSource("test")
                .setCatalog("catalog")
                .setSchema("schema")
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setRemoteUserAddress("address")
                .setUserAgent("agent")
                .build();
    }

    public RelationType getStatementTupleDescriptor(String sql, Session session)
    {
        // verify round-trip
        Statement statement;
        try {
            statement = sqlParser.createStatement(sql);
        }
        catch (ParsingException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Formatted query does not parse: " + sql);
        }

        Analysis analysis = analyzeStatement(statement, session);

        try {
            return analysis.getOutputDescriptor();
        }
        catch (SemanticException e) {
            if (e.getCode() == SemanticErrorCode.MISSING_TABLE) {
                return null;
            }
            else {
                throw e;
            }
        }
    }
}
