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
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.*;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.Kv;
import io.airlift.json.JsonCodec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.intellij.lang.annotations.Language;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.table;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Locale.ENGLISH;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static org.testng.Assert.fail;
import static java.lang.String.format;

/*
OutputNode
ProjectNode
TableScanNode
ValuesNode
AggregationNode
MarkDistinctNode
FilterNode
WindowNode
RowNumberNode
TopNRowNumberNode
LimitNode
DistinctLimitNode
TopNNode
SampleNode
SortNode
RemoteSourceNode
JoinNode
SemiJoinNode
IndexJoinNode
IndexSourceNode
TableWriterNode
DeleteNode
MetadataDeleteNode
TableCommitNode
UnnestNode
ExchangeNode
UnionNode


OutputNode

ProjectNode

ValuesNode
AggregationNode
MarkDistinctNode
FilterNode
WindowNode
RowNumberNode
TopNRowNumberNode
LimitNode
DistinctLimitNode
TopNNode
SampleNode
SortNode
RemoteSourceNode

JoinNode
SemiJoinNode
IndexJoinNode
 - implode

TableScanNode
IndexSourceNode
 - source

UnnestNode
 - explode

ExchangeNode

UnionNode
*/

public class TestInsertRewriter
{
    public static final Session SESSION = Session.builder(new SessionPropertyManager())
            .setSource("test")
            .setCatalog("default")
            .setSchema("default")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .setQueryId(QueryId.valueOf("dummy"))
            .setIdentity(new Identity("test", Optional.<Principal>empty()))
            .build();

    private static final SqlParser SQL_PARSER = new SqlParser();

    private Analyzer analyzer;

    @BeforeMethod(alwaysRun = true)
    public void setup()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        MetadataManager metadata = new MetadataManager(new FeaturesConfig().setExperimentalSyntaxEnabled(true), typeManager, new SplitManager(), new BlockEncodingManager(typeManager), new SessionPropertyManager(), new TablePropertyManager());
        metadata.addConnectorMetadata("tpch", "tpch", new TestingMetadata());
        metadata.addConnectorMetadata("c2", "c2", new TestingMetadata());
        metadata.addConnectorMetadata("c3", "c3", new TestingMetadata());
        AccessControl accessControl = new AllowAllAccessControl();

        SchemaTableName table1 = new SchemaTableName("default", "t1");
        metadata.createTable(SESSION, "tpch", new TableMetadata("tpch", new ConnectorTableMetadata(table1,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", BIGINT, false),
                        new ColumnMetadata("b", BIGINT, false),
                        new ColumnMetadata("c", BIGINT, false),
                        new ColumnMetadata("d", BIGINT, false)))));

        SchemaTableName table2 = new SchemaTableName("default", "t2");
        metadata.createTable(SESSION, "tpch", new TableMetadata("tpch", new ConnectorTableMetadata(table2,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", BIGINT, false),
                        new ColumnMetadata("b", BIGINT, false)))));

        SchemaTableName table3 = new SchemaTableName("default", "t3");
        metadata.createTable(SESSION, "tpch", new TableMetadata("tpch", new ConnectorTableMetadata(table3,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", BIGINT, false),
                        new ColumnMetadata("b", BIGINT, false),
                        new ColumnMetadata("x", BIGINT, false, null, true)))));

        // table in different catalog
        SchemaTableName table4 = new SchemaTableName("s2", "t4");
        metadata.createTable(SESSION, "c2", new TableMetadata("tpch", new ConnectorTableMetadata(table4,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", BIGINT, false)))));

        // table with a hidden column
        SchemaTableName table5 = new SchemaTableName("default", "t5");
        metadata.createTable(SESSION, "tpch", new TableMetadata("tpch", new ConnectorTableMetadata(table5,
                ImmutableList.<ColumnMetadata>of(
                        new ColumnMetadata("a", BIGINT, false),
                        new ColumnMetadata("b", BIGINT, false, null, true)))));

        // valid view referencing table in same schema
        String viewData1 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select a from t1",
                        Optional.of("tpch"),
                        Optional.of("default"),
                        ImmutableList.of(new ViewDefinition.ViewColumn("a", BIGINT)),
                        Optional.<String>empty()
                ));
        metadata.createView(SESSION, new QualifiedTableName("tpch", "default", "v1"), viewData1, false);

        // stale view (different column type)
        String viewData2 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition("select a from t1", Optional.of("tpch"), Optional.of("default"), ImmutableList.of(
                        new ViewDefinition.ViewColumn("a", VARCHAR)), Optional.<String>empty()));
        metadata.createView(SESSION, new QualifiedTableName("tpch", "default", "v2"), viewData2, false);

        // view referencing table in different schema from itself and session
        String viewData3 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition("select a from t4", Optional.of("c2"), Optional.of("s2"), ImmutableList.of(
                        new ViewDefinition.ViewColumn("a", BIGINT)), Optional.<String>empty()));
        metadata.createView(SESSION, new QualifiedTableName("c3", "s3", "v3"), viewData3, false);

        analyzer = new Analyzer(
                Session.builder(new SessionPropertyManager())
                        .setSource("test")
                        .setCatalog("tpch")
                        .setSchema("default")
                        .setTimeZoneKey(UTC_KEY)
                        .setLocale(ENGLISH)
                        .setQueryId(QueryId.valueOf("dummy"))
                        .setIdentity(new Identity("test", Optional.<Principal>empty()))
                        .build(),
                metadata,
                SQL_PARSER,
                accessControl,
                Optional.empty(),
                true);
    }

    public static LocalQueryRunner createLocalQueryRunner()
    {
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(SESSION);

        // add tpch
        InMemoryNodeManager nodeManager = localQueryRunner.getNodeManager();
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(nodeManager, 1), ImmutableMap.<String, String>of());

        // add raptor
        // ConnectorFactory raptorConnectorFactory = createRaptorConnectorFactory(TPCH_CACHE_DIR, nodeManager);
        // localQueryRunner.createCatalog("default", raptorConnectorFactory, ImmutableMap.<String, String>of());

        // Metadata metadata = localQueryRunner.getMetadata();
        // if (!metadata.getTableHandle(session, new QualifiedTableName("default", "default", "orders")).isPresent()) {
        //     localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.sf1.orders");
        // }
        // if (!metadata.getTableHandle(session, new QualifiedTableName("default", "default", "lineitem")).isPresent()) {
        //     localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.sf1.lineitem");
        // }
        return localQueryRunner;
    }

    @Test
    public void testOtherShit() throws Throwable
    {
        LocalQueryRunner lqr = createLocalQueryRunner();
        // lqr.execute

        LocalQueryRunner.MaterializedOutputFactory outputFactory = new LocalQueryRunner.MaterializedOutputFactory();

        /*
        {
            Kv<String, String> kv = new Kv.Synchronized<>(new Kv.MapImpl<>(new HashMap<>()));
            kv.put("hi", "there");
            kv.get("hi");

            TaskContext taskContext = createTaskContext(lqr.getExecutor(), lqr.getDefaultSession());
            List<Driver> drivers = lqr.createDrivers(lqr.getDefaultSession(), "select * from tpch.tiny.customer", outputFactory, taskContext);

            boolean done = false;
            while (!done) {
                boolean processed = false;
                for (Driver driver : drivers) {
                    if (!driver.isFinished()) {
                        driver.process();
                        processed = true;
                    }
                }
                done = !processed;
            }

            outputFactory.getMaterializingOperator().getMaterializedResult();
        }
        */

        {
            TaskContext taskContext = createTaskContext(lqr.getExecutor(), lqr.getDefaultSession());
            @Language("SQL") String sql = "select * from tpch.tiny.\"orders\" as \"o\" " +
                    "inner join tpch.tiny.customer as \"c1\" on \"o\".custkey = \"c1\".custkey " +
                    "inner join tpch.tiny.customer as \"c2\" on \"o\".custkey = (\"c2\".custkey + 1)";

            Statement statement = SQL_PARSER.createStatement(sql);
            Analyzer analyzer = new Analyzer(
                    lqr.getDefaultSession(),
                    lqr.getMetadata(),
                    SQL_PARSER,
                    lqr.getAccessControl(),
                    Optional.empty(),
                    true);

            Analysis analysis = analyzer.analyze(statement);
            System.out.println(analysis);

            // ----

            PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
            FeaturesConfig featuresConfig = new FeaturesConfig()
                    .setExperimentalSyntaxEnabled(true)
                    .setDistributedIndexJoinsEnabled(false)
                    .setOptimizeHashGeneration(true);
            PlanOptimizersFactory planOptimizersFactory = new PlanOptimizersFactory(
                    lqr.getMetadata(),
                    SQL_PARSER,
                    new IndexManager(), // ?
                    featuresConfig,
                    true);

            QueryExplainer queryExplainer = new QueryExplainer(
                    planOptimizersFactory.get(),
                    lqr.getMetadata(),
                    lqr.getAccessControl(),
                    SQL_PARSER,
                    ImmutableMap.of(),
                    featuresConfig.isExperimentalSyntaxEnabled());
            analyzer = new Analyzer(lqr.getDefaultSession(), lqr.getMetadata(), SQL_PARSER, lqr.getAccessControl(), Optional.of(queryExplainer), featuresConfig.isExperimentalSyntaxEnabled());

            analysis = analyzer.analyze(statement);
            Plan plan = new LogicalPlanner(lqr.getDefaultSession(), planOptimizersFactory.get(), idAllocator, lqr.getMetadata()).plan(analysis);

            List<TableScanNode> tableScanNodes = newArrayList();
            plan.getRoot().accept(new PlanVisitor<Void, Void>() {
                @Override
                protected Void visitPlan(PlanNode node, Void context)
                {
                    node.getSources().forEach(n -> n.accept(this, context));
                    return null;
                }

                @Override
                public Void visitTableScan(TableScanNode node, Void context)
                {
                    tableScanNodes.add(node);
                    return null;
                }
            }, null);

            System.out.println(PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), lqr.getMetadata(), lqr.getDefaultSession()));



            SubPlan subplan = new PlanFragmenter().createSubPlans(plan);
            if (!subplan.getChildren().isEmpty()) {
                throw new AssertionError("Expected subplan to have no children");
            }

            // ---

            List<Driver> drivers = lqr.createDrivers(lqr.getDefaultSession(), sql, outputFactory, taskContext);

            boolean done = false;
            while (!done) {
                boolean processed = false;
                for (Driver driver : drivers) {
                    if (!driver.isFinished()) {
                        driver.process();
                        processed = true;
                    }
                }
                done = !processed;
            }

            MaterializedResult res = outputFactory.getMaterializingOperator().getMaterializedResult();
            System.out.println(res);
        }
    }

    @Test
    public void testShit() throws Throwable
    {
        @Language("SQL") String queryStr = "select a, b from t1";

        Statement statement = SQL_PARSER.createStatement(queryStr);
        Analysis analysis = analyzer.analyze(statement);

        InsertRewriter rewriter = new InsertRewriter(analysis);
        rewriter.process(statement, null);

        Field pk = analysis.getOutputDescriptor().getFieldByIndex(0);
        Query query = analysis.getQuery();
        QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
        Table table =  (Table) querySpecification.getFrom().get();

        Insert insert = new Insert(QualifiedName.of("t2"), query);
        String insertStr = SqlFormatter.formatSql(insert);
        System.out.println(insertStr);

        Analysis insertAnalysis = analyzer.analyze(insert);

        LocalQueryRunner lqr = createLocalQueryRunner();
        // lqr.execute

        LocalQueryRunner.MaterializedOutputFactory outputFactory = new LocalQueryRunner.MaterializedOutputFactory();

        TaskContext taskContext = createTaskContext(lqr.getExecutor(), lqr.getDefaultSession());
        List<Driver> drivers = lqr.createDrivers(lqr.getDefaultSession(), insert, outputFactory, taskContext);

        boolean done = false;
        while (!done) {
            boolean processed = false;
            for (Driver driver : drivers) {
                if (!driver.isFinished()) {
                    driver.process();
                    processed = true;
                }
            }
            done = !processed;
        }

        outputFactory.getMaterializingOperator().getMaterializedResult();
    }
}