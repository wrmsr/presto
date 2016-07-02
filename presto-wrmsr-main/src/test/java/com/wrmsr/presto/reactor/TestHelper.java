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
package com.wrmsr.presto.reactor;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.transaction.LegacyConnectorMetadata;
import com.facebook.presto.transaction.LegacyTransactionConnector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.wrmsr.presto.connector.jdbc.ExtendedJdbcConnectorFactory;
import com.wrmsr.presto.connectorSupport.ExtendedJdbcConnectorSupport;
import com.wrmsr.presto.spi.connectorSupport.ConnectorSupport;
import com.wrmsr.presto.tpch.TpchConnectorSupport;
import org.intellij.lang.annotations.Language;

import java.io.File;
import java.security.Principal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class TestHelper
{
    public final Session session = Session.builder(new SessionPropertyManager())
            .setSource("test")
            .setCatalog("default")
            .setSchema("default")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .setQueryId(QueryId.valueOf("dummy"))
            .setIdentity(new Identity("test", Optional.<Principal>empty()))
            .build();

    public final SqlParser sqlParser = new SqlParser();

    public LocalQueryRunner createLocalQueryRunner()
    {
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session);

        // add tpch
        InMemoryNodeManager nodeManager = localQueryRunner.getNodeManager();
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(nodeManager, 1), ImmutableMap.<String, String>of());

        final ConnectorSession session = new TestingConnectorSession(
                "user",
                UTC_KEY,
                ENGLISH,
                System.currentTimeMillis(),
                ImmutableList.of(),
                ImmutableMap.of());

        File tmp = Files.createTempDir();
        tmp.deleteOnExit();
        File db = new File(tmp, "db");
        ExtendedJdbcConnectorFactory connectorFactory = new ExtendedJdbcConnectorFactory(
                "test",
                new TestingH2JdbcModule(),
                TestingH2JdbcModule.createProperties(db),
                ImmutableMap.of(),
                TestHelper.class.getClassLoader());

        localQueryRunner.createCatalog("test", connectorFactory, ImmutableMap.<String, String>of());

        Connector connector =
                new LegacyTransactionConnector("test", connectorFactory.create("test", TestingH2JdbcModule.createProperties(db)));
        ConnectorMetadata metadata = connector.getMetadata(connector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, false));
        JdbcMetadata jdbcMetadata = (JdbcMetadata) ((LegacyConnectorMetadata) metadata).getMetadata();
        BaseJdbcClient jdbcClient = (BaseJdbcClient) jdbcMetadata.getJdbcClient();
        try {
            try (Connection connection = jdbcClient.getConnection()) {
                try (java.sql.Statement stmt = connection.createStatement()) {
                    stmt.execute("CREATE SCHEMA test");
                    stmt.execute("CREATE TABLE test.foo (id integer primary key)");
                }
                // connection.createStatement().execute("CREATE TABLE example.foo (id integer primary key)");
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return localQueryRunner;
    }

    public class PlannedQuery
    {
        public final Statement statement;
        public final LocalQueryRunner lqr;
        public final Session session;
        public final PlanNodeIdAllocator idAllocator;
        public final FeaturesConfig featuresConfig;
        public final PlanOptimizersFactory planOptimizersFactory;
        public final QueryExplainer queryExplainer;
        public final Analyzer analyzer;
        public final Analysis analysis;
        public final List<PlanOptimizer> planOptimizers;
        public final LogicalPlanner planner;
        public final Plan plan;

        public final Map<String, Connector> connectors;
        public final Map<String, ConnectorSupport> connectorSupport;

        public PlannedQuery(@Language("SQL") String sql)
        {
            statement = sqlParser.createStatement(sql);
            lqr = createLocalQueryRunner();

            session = lqr.getDefaultSession().withTransactionId(lqr.getTransactionManager().beginTransaction(true));

            idAllocator = new PlanNodeIdAllocator();

            featuresConfig = new FeaturesConfig()
                    .setExperimentalSyntaxEnabled(true)
                    .setDistributedIndexJoinsEnabled(false)
                    .setOptimizeHashGeneration(true);

            planOptimizersFactory = new PlanOptimizersFactory(
                    lqr.getMetadata(),
                    sqlParser,
                    featuresConfig,
                    true);

            queryExplainer = new QueryExplainer(
                    planOptimizersFactory.get(),
                    lqr.getMetadata(),
                    lqr.getAccessControl(),
                    sqlParser,
                    ImmutableMap.of(),
                    featuresConfig.isExperimentalSyntaxEnabled());

            analyzer = new Analyzer(
                    session,
                    lqr.getMetadata(),
                    sqlParser,
                    lqr.getAccessControl(),
                    Optional.of(queryExplainer),
                    featuresConfig.isExperimentalSyntaxEnabled());

            analysis = analyzer.analyze(statement);

            planOptimizers = planOptimizersFactory.get();

            planner = new LogicalPlanner(
                    session,
                    planOptimizers,
                    idAllocator,
                    lqr.getMetadata(),
                    sqlParser
            );

            plan = planner.plan(analysis);

            connectors = lqr.getConnectorManager().getConnectors();

            connectorSupport = ImmutableMap.<String, ConnectorSupport>builder()
                    .put("tpch", new TpchConnectorSupport(session.toConnectorSession(), connectors.get("tpch"))) // , connectors.get("tpch"), "tiny"))
                    .put("test", new ExtendedJdbcConnectorSupport(session.toConnectorSession(), connectors.get("test"), ((LegacyTransactionConnector) connectors.get("test")).getConnector()))
                    .build();
        }
    }

    public PlannedQuery plan(@Language("SQL") String sql)
            throws Throwable
    {
        return new PlannedQuery(sql);
    }
}
