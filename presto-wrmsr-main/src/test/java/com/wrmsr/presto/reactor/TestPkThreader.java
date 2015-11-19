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

/*
k's:
 primary
 secondary
 non
 join
 group

TODO optional whole-table buffering

split whenever out-pk != in-pk
CANNOT select from TableScans
events include AT LEAST pre-pk and FULL postimage
 - could be driven by post-deltas with TableScan buf
 - and fuck it just do full both at first ugh

TODO pk+sk isp tables, wide-rows - split tbls up
 - interim just use fuckin __data__ blobz

n-way join plz, no hash opt
 - just manually add lol

drop tablescan predis, need it all
 - predicate pushup lols

 List<List<PlanNodeId>> populationPlanStages
 Map<ImmutablePair<ConnectorId, SchemaTableName>, List<PlanNodeId>> reactionPlanLists
*/

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.reactor.tuples.Layout;
import com.wrmsr.presto.reactor.tuples.PkLayout;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableSet;
import static jersey.repackaged.com.google.common.collect.Lists.newArrayList;

public class TestPkThreader
{
    public final TestHelper helper = new TestHelper();

    @FunctionalInterface
    public interface IntermediateStorageProvider
    {
        TableHandle getIntermediateStorage(String name, PkLayout<String> layout);
    }

    public static class PkThreader
            extends PlanVisitor<PkThreader.Context, PlanNode>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Session session;
        private final Metadata metadata;
        private final List<PlanOptimizer> planOptimizers;
        private final Map<Symbol, Type> types;
        private final Map<String, ConnectorSupport> connectorSupport;
        private final IntermediateStorageProvider intermediateStorageProvider;

        public PkThreader(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session, Metadata metadata, List<PlanOptimizer> planOptimizers, Map<Symbol, Type> types, Map<String, ConnectorSupport> connectorSupport, IntermediateStorageProvider intermediateStorageProvider)
        {
            this.idAllocator = idAllocator;
            this.symbolAllocator = symbolAllocator;
            this.session = session;
            this.metadata = metadata;
            this.planOptimizers = planOptimizers;
            this.types = types;
            this.connectorSupport = connectorSupport;
            this.intermediateStorageProvider = intermediateStorageProvider;
        }

        public abstract static class Action
        {
            private final OutputNode root;

            public Action(OutputNode root)
            {
                this.root = root;
            }
        }

        /*
        public static class Reaction extends Action
        {
        }
        */

        public static class Population extends Action
        {
            private final TableHandle output;

            public Population(OutputNode root, TableHandle output)
            {
                super(root);
                this.output = output;
            }
        }

        public static class Context
        {
            private final PkThreader owner;
            private final Map<PlanNodeId, NodeInfo> nodeInfo;
            private final Map<PkLayout<Symbol>, PkLayout<Symbol>> layoutCache;

            private Context(PkThreader owner)
            {
                this.owner = owner;
                nodeInfo = newHashMap();
                layoutCache = newHashMap();
            }

            private PlanNode registerNode(PlanNode node, List<Symbol> pkSyms, List<Population> populations)
            {
                checkArgument(!nodeInfo.containsKey(node.getId()));

                Map<Symbol, Type> types = owner.symbolAllocator.getTypes();
                PkLayout<Symbol> layout = new PkLayout<>(
                        node.getOutputSymbols(),
                        node.getOutputSymbols().stream().map(types::get).collect(toImmutableList()),
                        pkSyms);
                layoutCache.putIfAbsent(layout, layout);
                layout = layoutCache.get(layout);

                NodeInfo info = new NodeInfo(node, layout, populations);
                nodeInfo.put(node.getId(), info);
                return node;
            }

            private NodeInfo addChild(PlanNode node)
            {
                PlanNode newNode = node.accept(owner, this);
                return nodeInfo.get(newNode.getId());
            }
        }

        private static class NodeInfo
        {
            private final PlanNode node;
            private final PkLayout<Symbol> layout;
            private final List<Population> populations;

            public NodeInfo(PlanNode node, PkLayout<Symbol> layout, List<Population> populations)
            {
                this.node = node;
                this.layout = layout;
                this.populations = populations;
            }

            private List<Symbol> pkSyms()
            {
                return layout.getPkNames();
            }
        }

        private static final String dataColumnName = "__data__";
        private static final Layout.Field<String> dataField = new Layout.Field<>(dataColumnName, VarbinaryType.VARBINARY);

        public Context newContext()
        {
            return new Context(this);
        }

        protected PlanNode visitPlan(PlanNode node, Context context)
        {
            throw new UnsupportedPlanNodeException(node);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Context context)
        {
            NodeInfo newSource = context.addChild(node.getSource());

            Set<QualifiedNameReference> pkQnrs = newSource.pkSyms().stream().map(Symbol::toQualifiedNameReference).collect(toImmutableSet());
            Set<Symbol> identityAssignments = node.getAssignments().entrySet().stream().filter(e -> pkQnrs.contains(e.getValue())).map(Map.Entry::getKey).collect(toImmutableSet());

            Map<Symbol, Expression> newAssignments = newHashMap(node.getAssignments());
            for (Symbol pkSym : newSource.pkSyms()) {
                if (!identityAssignments.contains(pkSym)) {
                    newAssignments.put(pkSym, pkSym.toQualifiedNameReference());
                }
            }

            ProjectNode newNode = new ProjectNode(
                    node.getId(),
                    newSource.node,
                    newAssignments);
            return context.registerNode(newNode, newSource.pkSyms(), ImmutableList.of());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Context context)
        {
            NodeInfo newSource = context.addChild(node.getSource());

            FilterNode newNode = new FilterNode(
                    node.getId(),
                    newSource.node,
                    node.getPredicate());
            return context.registerNode(newNode, newSource.pkSyms(), ImmutableList.of());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, Context context)
        {
            // !!! on lpk = rpk -> fuse pk's into one sym
            NodeInfo newLeft = context.addChild(node.getLeft());
            NodeInfo newRight = context.addChild(node.getRight());

            /*
            List<JoinNode.EquiJoinClause> nonHashClauses = node.getCriteria().stream()
                    .filter(c -> !(node.getLeftHashSymbol().isPresent() && c.getLeft().equals(node.getLeftHashSymbol().get())))
                    .filter(c -> !(node.getRightHashSymbol().isPresent() && c.getRight().equals(node.getRightHashSymbol().get())))
                    .collect(toImmutableList());
            List<Symbol> leftJkSyms = nonHashClauses.stream()
                    .map(JoinNode.EquiJoinClause::getLeft)
                    .filter(s -> !leftLayout.getPk().containsName(s))
                    .collect(toImmutableList());
            List<Symbol> rightJkSyms = nonHashClauses.stream()
                    .map(JoinNode.EquiJoinClause::getRight)
                    .filter(s -> !rightLayout.getPk().containsName(s))
                    .collect(toImmutableList());
            */

            List<Symbol> pkSyms = ImmutableList.<Symbol>builder()
                    .addAll(newLeft.pkSyms())
                    .addAll(newRight.pkSyms())
                    .build();
            checkState(pkSyms.size() == newHashSet(pkSyms).size());

            JoinNode newNode = new JoinNode(
                    node.getId(),
                    node.getType(),
                    newLeft.node,
                    newRight.node,
                    node.getCriteria(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol());

            List<Population> populations = newArrayList();

            OutputNode indexPopulationRoot = new OutputNode(
                    idAllocator.getNextId(),
                    newNode,
                    pkSyms.stream().map(Symbol::getName).collect(toImmutableList()),
                    pkSyms);
            indexPopulationRoot = (OutputNode) optimize(indexPopulationRoot);

//            need project to struct of pk
//            PlanNode leftIndexAgg = new AggregationNode(
//                    idAllocator.getNextId(),
//                    newNode,
//                    newLeft.pkSyms(),
//                    ImmutableMap.builder()
//
//
//            @JsonProperty("aggregations") Map<Symbol, FunctionCall> aggregations,
//            @JsonProperty("functions") Map<Symbol, Signature> functions,
//            @JsonProperty("masks") Map<Symbol, Symbol> masks,
//            @JsonProperty("step") Step step,
//            @JsonProperty("sampleWeight") Optional<Symbol> sampleWeight,
//            @JsonProperty("confidence") double confidence,
//            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol)
//
//            );

            // lpk -> [rpk]
            TableHandle leftIndexTableHandle = intermediateStorageProvider.getIntermediateStorage(
                    String.format("%s_left_index", node.getId().toString()),
                    new PkLayout<>(
                            newLeft.layout.getPk().mapNames(Symbol::getName).getFields(),
                            ImmutableList.of(dataField)));
            populations.add(new Population(indexPopulationRoot, leftIndexTableHandle));

            TableHandle leftDataTableHandle = intermediateStorageProvider.getIntermediateStorage(
                    String.format("%s_left_data", node.getId().toString()),
                    newLeft.layout.mapNames(Symbol::getName));

            context.registerNode(newNode, pkSyms, populations);
            return newNode;
        }

        @Override
        public PlanNode visitOutput(OutputNode node, Context context)
        {
            NodeInfo newSource = context.addChild(node.getSource());

            Set<Symbol> outputSymbolSet = newHashSet(node.getOutputSymbols());
            List<String> newColumnNames = newArrayList(node.getColumnNames());
            List<Symbol> newOutputSymbols = newArrayList(node.getOutputSymbols());

            for (Symbol pkSym : newSource.pkSyms()) {
                if (!outputSymbolSet.contains(pkSym)) {
                    String pkCol = pkSym.getName();
                    checkState(!newColumnNames.contains(pkCol));
                    newColumnNames.add(pkCol);
                    newOutputSymbols.add(pkSym);
                }
            }

            OutputNode newNode = new OutputNode(
                    node.getId(),
                    newSource.node,
                    newColumnNames,
                    newOutputSymbols);
            return context.registerNode(newNode, newSource.pkSyms(), ImmutableList.of());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Context context)
        {
            // FIXME (optional?) filter extraction - we (may?) only get deltas
            ConnectorSupport<?> cs = connectorSupport.get(node.getTable().getConnectorId());
            Connector c = cs.getConnector();
            ConnectorSession csess = session.toConnectorSession();
            SchemaTableName stn = cs.getSchemaTableName(node.getTable().getConnectorHandle());
            ConnectorTableHandle th = c.getMetadata().getTableHandle(csess, stn);
            Map<String, ColumnHandle> chs = c.getMetadata().getColumnHandles(csess, th);
            // PkTableTupleLayout l = cs.getTableTupleLayout(stn);

            List<String> pkCols = cs.getPrimaryKey(stn);
            Map<String, Symbol> colSyms = node.getAssignments().entrySet().stream().map(e -> ImmutablePair.of(cs.getColumnName(e.getValue()), e.getKey())).collect(toImmutableMap());

            Map<Symbol, ColumnHandle> newAssignments = newHashMap(node.getAssignments());
            List<Symbol> newOutputSymbols = newArrayList(node.getOutputSymbols());
            List<Symbol> pkSyms = newArrayList();

            for (String pkCol : pkCols) {
                if (colSyms.containsKey(pkCol)) {
                    pkSyms.add(colSyms.get(pkCol));
                }
                else {
                    ColumnHandle ch = chs.get(pkCol);
                    ColumnMetadata cm = c.getMetadata().getColumnMetadata(csess, th, ch);
                    Symbol pkSym = symbolAllocator.newSymbol(pkCol, cm.getType());

                    newAssignments.put(pkSym, ch);
                    newOutputSymbols.add(pkSym);
                    pkSyms.add(pkSym);
                }
            }

            TableScanNode newNode = new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    newOutputSymbols,
                    newAssignments,
                    node.getLayout(),
                    node.getCurrentConstraint(),
                    node.getOriginalConstraint());
            return context.registerNode(newNode, pkSyms, ImmutableList.of());
        }

        private PlanNode optimize(PlanNode planNode)
        {
            for (PlanOptimizer planOptimizer : planOptimizers) {
                planNode = planOptimizer.optimize(
                        planNode,
                        session,
                        types,
                        symbolAllocator,
                        idAllocator);
            }
            return planNode;
        }

//        @Override
//        public PlanNode visitAggregation(AggregationNode node, Context context)
//        {
//            NodeInfo newSource = node.getSource().accept(this, context);
//            List<Symbol> pkSyms = context.nodePkSyms.get(backingSource.getId());
//            Set<Symbol> pkSymSet = newHashSet(pkSyms);
//            checkState(pkSymSet.size() == pkSyms.size());
//
//            /*
//            AggregationNode newNode = new AggregationNode(
//                    node.getId(),
//                    node.getSource(),
//                    node.getGroupBy(),
//                    node.getAggregations(),
//                    node.getFunctions(),
//                    node.getMasks()
//                    node.getSampleWeight(),
//                    node.getConfidence());
//            return newNode;
//            */
//
//            List<Symbol> gkSyms = node.getGroupBy();
//            Set<Symbol> gkSymSet = newHashSet(gkSyms);
//            List<Symbol> nonGkSyms = node.getOutputSymbols().stream().filter(s -> !gkSymSet.contains(s)).collect(toImmutableList());
//            List<Symbol> nonGkPkSyms = pkSyms.stream().filter(s -> !gkSymSet.contains(s)).collect(toImmutableList());
//            List<Symbol> nonPkGkSyms = gkSyms.stream().filter(s -> !pkSymSet.contains(s)).collect(toImmutableList());
//
//            // FIXME optimize away if gk is pk
//            TableHandle indexTableHandle = intermediateStorageProvider.getIntermediateStorage(
//                    String.format("%s_index", node.getId().toString()),
//                    new PkLayout(
//                            toFields(pkSyms),
//                            toFields(nonPkGkSyms)));
//            ConnectorSupport indexTableConnectorSupport = connectorSupport.get(indexTableHandle.getConnectorId());
//            Connector indexTableConnector = indexTableConnectorSupport.getConnector();
//            Map<String, ColumnHandle> indexTableColumnHandles = indexTableConnector.getMetadata().getColumnHandles(session.toConnectorSession(), indexTableHandle.getConnectorHandle());
//
//            // TODO optional log(n) recombine, wide rows, special-case reversible combiners (count, sum, array, map)
//            TableHandle dataTableHandle = intermediateStorageProvider.getIntermediateStorage(
//                    String.format("%s_data", node.getId().toString()),
//                    new PkLayout(
//                            toFields(gkSyms),
//                            Stream.concat(toFields(nonGkSyms).stream(), Stream.of(new Layout.Field(dataColumnName, VarbinaryType.VARBINARY))).collect(toImmutableList())));
//            ConnectorSupport dataTableConnectorSupport = connectorSupport.get(dataTableHandle.getConnectorId());
//            Connector dataTableConnector = dataTableConnectorSupport.getConnector();
//            Map<String, ColumnHandle> dataTableColumnHandles = dataTableConnector.getMetadata().getColumnHandles(session.toConnectorSession(), dataTableHandle.getConnectorHandle());
//
//            List<Symbol> gkPkSyms = Stream.concat(gkSyms.stream(), nonGkPkSyms.stream()).collect(toImmutableList());
//            PlanNode indexQueryRoot = new OutputNode(
//                    idAllocator.getNextId(),
//                    new ProjectNode(
//                            idAllocator.getNextId(),
//                            backingSource,
//                            gkPkSyms.stream().map(s -> ImmutablePair.of(s, (Expression) s.toQualifiedNameReference())).collect(toImmutableMap())),
//                    gkPkSyms.stream().map(Symbol::getName).collect(toImmutableList()),
//                    gkPkSyms);
//            indexQueryRoot = optimize(indexQueryRoot);
//
//            Map<Symbol, FunctionCall> newAggregations = newHashMap(node.getAggregations());
//            Map<Symbol, Signature> newFunctions = newHashMap(node.getFunctions());
//            for (Symbol aggSym : newAggregations.keySet()) {
//
//            }
//
//            PlanNode dataQueryRoot = new OutputNode(
//                    idAllocator.getNextId(),
//                    new AggregationNode(
//                            idAllocator.getNextId(),
//                            backingSource,
//                            node.getGroupBy(),
//                            newAggregations,
//                            newFunctions,
//                            node.getMasks(),
//                            node.getStep(),
//                            node.getSampleWeight(),
//                            node.getConfidence(),
//                            node.getHashSymbol()),
//                    node.getAggregations().keySet().stream().map(Symbol::getName).collect(toImmutableList()),
//                    newArrayList(node.getAggregations().keySet())
//            );
//            dataQueryRoot = optimize(dataQueryRoot);
//
//            Map<Symbol, ColumnHandle> newAssignments = node.getOutputSymbols().stream().map(s -> ImmutablePair.of(s, dataTableColumnHandles.get(s.getName()))).collect(toImmutableMap());
//
//            TableScanNode newNode = new TableScanNode(
//                    idAllocator.getNextId(),
//                    dataTableHandle,
//                    node.getOutputSymbols(),
//                    newAssignments,
//                    Optional.<TableLayoutHandle>empty(),
//                    TupleDomain.all(),
//                    null);
//            context.nodePkSyms.put(newNode.getId(), gkSyms);
//            return newNode;
//        }
    }

    @Test
    public void testThing()
            throws Throwable
    {
        @Language("SQL") String stmt =

                "select customer.name, nation.name from tpch.tiny.customer inner join tpch.tiny.nation on customer.nationkey = nation.nationkey where acctbal > 100"

                // "select nationkey, count(*) c from tpch.tiny.customer where acctbal > 10 group by nationkey"

                // "select name, customer_names from tpch.tiny.nation inner join (select nationkey, sum(length(customer.name)) customer_names from tpch.tiny.customer where acctbal > 10 group by nationkey) customers on nation.nationkey = customers.nationkey"

                ;

        TestHelper.PlannedQuery pqa = helper.plan(

        "select nationkey, array_agg(name) from tpch.tiny.customer group by nationkey"

        );

        TestHelper.PlannedQuery pq = helper.plan(stmt);

        Function<Type, String> formatSqlCol = t -> {
            if (t instanceof BigintType) {
                return "bigint";
            }
            else if (t instanceof VarbinaryType) {
                return "varbinary";
            }
            else if (t instanceof VarcharType) {
                return "varchar";
            }
            else {
                throw new UnsupportedOperationException();
            }
        };

        IntermediateStorageProvider isp = (n, l) -> {
            String schemaName = "example";
            String tableName = "isp_" + n;

            StringBuilder sql = new StringBuilder();
            sql.append("create table `" + schemaName + "`.`" + tableName + "` (");
            sql.append(l.getFields().stream()
                    .map(c -> String.format("`%s` %s", c.getName(), formatSqlCol.apply(c.getType())))
                    .collect(Collectors.joining(", ")));

            sql.append(", primary key (");
            sql.append(l.getPk().getFields().stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sql.append(")");

            sql.append(");");

            String connectorId = "test";
            JdbcMetadata jdbcMetadata = (JdbcMetadata) pq.connectors.get(connectorId).getMetadata();
            BaseJdbcClient jdbcClient = (BaseJdbcClient) jdbcMetadata.getJdbcClient();
            try {
                try (Connection sqlConn = jdbcClient.getConnection()) {
                    try (Statement sqlStmt = sqlConn.createStatement()) {
                        try {
                            sqlStmt.execute("create schema `" + schemaName + "`;");
                        }
                        catch (SQLException e) {
                        }
                        sqlStmt.execute(sql.toString());
                    }
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }

            return new TableHandle(connectorId, jdbcMetadata.getTableHandle(pq.session.toConnectorSession(), new SchemaTableName(schemaName, tableName)));
        };

        PkThreader r = new PkThreader(
                pq.idAllocator,
                pq.planner.getSymbolAllocator(),
                pq.session,
                pq.lqr.getMetadata(),
                pq.planOptimizers,
                pq.plan.getTypes(),
                pq.connectorSupport,
                isp);
        PkThreader.Context ctx = r.newContext();

        PlanNode newRoot = pq.plan.getRoot().accept(r, ctx);
        System.out.println(newRoot);
        System.out.println(ctx);

        /*
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
        */
    }
}
