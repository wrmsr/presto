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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;

/*


             xxxxxxx
        x xxxxxxxxxxxxx x
     x     xxxxxxxxxxx     x
            xxxxxxxxx
  x          xxxxxxx          x
              xxxxx
 x             xxx             x
                x
xxxxxxxxxxxxxxx   xxxxxxxxxxxxxxx
 xxxxxxxxxxxxx     xxxxxxxxxxxxx
  xxxxxxxxxxx       xxxxxxxxxxx
   xxxxxxxxx         xxxxxxxxx
     xxxxxx           xxxxxx
       xxx             xxx
           x         x
                x



OutputNode

ProjectNode

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

AggregationNode
 - implode
 pk -> gk
 gk -> [pk -> data]
  => row

JoinNode
SemiJoinNode
IndexJoinNode
 - implode
  pk -> jk
  jk -> [pk -> data]
   => row[]

TableScanNode
IndexSourceNode
ValuesNode
 - source
 src pk multiplied

UnnestNode
 - explode

ExchangeNode

UnionNode
*/

//public class TestInsertRewriter
//{
//    private final TestHelper helper = new TestHelper();
//
//    public static class ReactorContext
//    {
//        private final Plan plan;
//        private final Analysis analysis;
//        private final Session sesion;
//        private final Metadata metadata;
//
//        private final Map<String, Connector> connectors;
//        private final Map<String, ConnectorSupport> connectorSupport;
//
//        private final Map<PlanNodeId, Reactor> reactors;
//
//        public ReactorContext(Plan plan, Analysis analysis, Session session, Metadata metadata, Map<String, Connector> connectors, Map<String, ConnectorSupport> connectorSupport)
//        {
//            this.plan = plan;
//            this.analysis = analysis;
//            this.sesion = session;
//            this.metadata = metadata;
//            this.connectors = connectors;
//            this.connectorSupport = connectorSupport;
//            reactors = newHashMap();
//        }
//
//        public Plan getPlan()
//        {
//            return plan;
//        }
//
//        public Analysis getAnalysis()
//        {
//            return analysis;
//        }
//
//        public Session getSesion()
//        {
//            return sesion;
//        }
//
//        public Metadata getMetadata()
//        {
//            return metadata;
//        }
//
//        public BlockEncodingSerde getBlockEncodingSerde()
//        {
//            return metadata.getBlockEncodingSerde();
//        }
//
//        public Map<String, Connector> getConnectors()
//        {
//            return connectors;
//        }
//
//        public Map<String, ConnectorSupport> getConnectorSupport()
//        {
//            return connectorSupport;
//        }
//
//        public Map<PlanNodeId, Reactor> getReactors()
//        {
//            return reactors;
//        }
//
//        public List<InputNodeReactor> getInputReactors()
//        {
//            return reactors.values().stream().filter(InputNodeReactor.class::isInstance).map(InputNodeReactor.class::cast).collect(toImmutableList());
//        }
//
//        public Kv<byte[], byte[]> getKv()
//        {
//            return new Kv.KeyCodec<>(new Kv.FromMap<>(newHashMap()), ByteArrayWrapper.CODEC);
//        }
//    }
//
//    public static abstract class Reactor<T extends PlanNode>
//    {
//        protected final T node;
//        protected final ReactorContext context;
//        protected final Optional<Reactor> destination;
//
//        public Reactor(T node, ReactorContext context, Optional<Reactor> destination)
//        {
//            this.node = node;
//            this.context = context;
//            this.destination = destination;
//        }
//
//        public abstract void react(TableEvent event);
//
//        protected TableTupleLayout nodeLayout(PlanNode node)
//        {
//            Map<Symbol, Type> typeMap = context.plan.getSymbolAllocator().getTypes();
//            List<String> names = node.getOutputSymbols().stream().map(s -> s.getName()).collect(toImmutableList());
//            List<Type> types = names.stream().map(n -> typeMap.get(new Symbol(n))).collect(toImmutableList());
//            return new TableTupleLayout(names, types);
//        }
//
//        protected PkTableTupleLayout nodeLayout(PlanNode node, List<String> pks)
//        {
//            TableTupleLayout layout = nodeLayout(node);
//            return new PkTableTupleLayout(layout.getNames(), layout.getTypes(), pks);
//        }
//
//        protected List<String> tablePkCols(TableHandle th)
//        {
//            ConnectorSupport cs = context.getConnectorSupport().get(th.getConnectorId());
//            SchemaTableName stn = cs.getSchemaTableName(th.getConnectorHandle());
//            return cs.getPrimaryKey(stn);
//        }
//
//        protected List<String> tablePks(TableHandle th, Map<Symbol, ColumnHandle> assignments)
//        {
//            ConnectorSupport cs = context.connectorSupport.get(th.getConnectorId());
//            List<String> pkCols = tablePkCols(th);
//            Map<String, Symbol> m = assignments.entrySet().stream().map(e -> ImmutablePair.of(cs.getColumnName(e.getValue()), e.getKey())).collect(toImmutableMap());
//            return pkCols.stream().map(c -> m.get(c).getName()).collect(toImmutableList());
//        }
//
//        public List<Reactor> getSources()
//        {
//            return node.getSources().stream().map(n -> context.reactors.get(n.getId())).collect(toImmutableList());
//        }
//
//        public Reactor getSource()
//        {
//            List<Reactor> sources = getSources();
//            checkState(sources.size() == 1);
//            return sources.get(0);
//        }
//    }
//
//    public static abstract class InputNodeReactor<T extends PlanNode>
//            extends Reactor<T>
//    {
//        public InputNodeReactor(T node, ReactorContext context, Optional<Reactor> destination)
//        {
//            super(node, context, destination);
//            checkArgument(destination.isPresent());
//        }
//    }
//
//    public static abstract class InnerNodeReactor<T extends PlanNode>
//            extends Reactor<T>
//    {
//        public InnerNodeReactor(T node, ReactorContext context, Optional<Reactor> destination)
//        {
//            super(node, context, destination);
//            checkArgument(destination.isPresent());
//        }
//    }
//
//    public static class OutputNodeReactor
//            extends Reactor<OutputNode>
//    {
//        // private final Layout layout;
//        private final Kv<byte[], byte[]> SImpleMap;
//
//        public OutputNodeReactor(OutputNode node, ReactorContext context, Optional<Reactor> destination)
//        {
//            super(node, context, destination);
//            checkArgument(!destination.isPresent());
//            // layout = nodeLayout(node);
//            SImpleMap = context.getKv(); }
//
//        @Override
//        public void react(TableEvent event)
//        {
//            BlockEncodingSerde bes = context.getBlockEncodingSerde();
//            switch (event.getOperation()) {
//                case INSERT: {
//                    PkTableTuple after = event.getAfter().get();
//                    SImpleMap.put(after.getPk().toBytes(bes), after.getNonPk().toBytes(bes));
//                    break;
//                }
//                case UPDATE: {
//                    PkTableTuple before = event.getBefore().get();
//                    SImpleMap.remove(before.getPk().toBytes(bes));
//                    PkTableTuple after = event.getAfter().get();
//                    SImpleMap.put(after.getPk().toBytes(bes), after.getNonPk().toBytes(bes));
//                    break;
//                }
//                case DELETE: {
//                    PkTableTuple before = event.getBefore().get();
//                    SImpleMap.remove(before.toBytes(bes));
//                    break;
//                }
//            }
//        }
//    }
//
//    @FunctionalInterface
//    public interface SymbolIndexResolver
//    {
//        int getSymbolIndex(Symbol symbol);
//    }
//
//    public static class RecordCursorSymbolResolver
//            implements RecordCursor, SymbolResolver
//    {
//        private final RecordCursor recordCursor;
//        private final SymbolIndexResolver symbolIndexResolver;
//
//        public RecordCursorSymbolResolver(RecordCursor recordCursor, SymbolIndexResolver symbolIndexResolver)
//        {
//            this.recordCursor = recordCursor;
//            this.symbolIndexResolver = symbolIndexResolver;
//        }
//
//        @Override
//        public Object getValue(Symbol symbol)
//        {
//            return getObject(symbolIndexResolver.getSymbolIndex(symbol));
//        }
//
//        @Override
//        public long getTotalBytes()
//        {
//            return recordCursor.getTotalBytes();
//        }
//
//        @Override
//        public long getCompletedBytes()
//        {
//            return recordCursor.getCompletedBytes();
//        }
//
//        @Override
//        public long getReadTimeNanos()
//        {
//            return recordCursor.getReadTimeNanos();
//        }
//
//        @Override
//        public Type getType(int field)
//        {
//            return recordCursor.getType(field);
//        }
//
//        @Override
//        public boolean advanceNextPosition()
//        {
//            return recordCursor.advanceNextPosition();
//        }
//
//        @Override
//        public boolean getBoolean(int field)
//        {
//            return recordCursor.getBoolean(field);
//        }
//
//        @Override
//        public long getLong(int field)
//        {
//            return recordCursor.getLong(field);
//        }
//
//        @Override
//        public double getDouble(int field)
//        {
//            return recordCursor.getDouble(field);
//        }
//
//        @Override
//        public Slice getSlice(int field)
//        {
//            return recordCursor.getSlice(field);
//        }
//
//        @Override
//        public Object getObject(int field)
//        {
//            return recordCursor.getObject(field);
//        }
//
//        @Override
//        public boolean isNull(int field)
//        {
//            return recordCursor.isNull(field);
//        }
//
//        @Override
//        public void close()
//        {
//            recordCursor.close();
//        }
//    }
//
//    public static class ProjectNodeReactor
//            extends InnerNodeReactor<ProjectNode>
//    {
//        private final Map<Symbol, ExpressionInterpreter> expressionInterpreters;
//
//        public ProjectNodeReactor(ProjectNode node, ReactorContext context, Optional<Reactor> destination)
//        {
//            super(node, context, destination);
//
//            Map<Expression, Type> expressionTypes = ImmutableMap.copyOf(context.getAnalysis().getTypes());
//            expressionInterpreters = node.getAssignments().entrySet().stream()
//                    .map(e -> ImmutablePair.of(
//                            e.getKey(),
//                            // FIXME: duplicate names?
//                            ExpressionInterpreter.expressionInterpreterUnsafe(
//                                    e.getValue(),
//                                    context.getMetadata(),
//                                    context.getSesion(),
//                                    expressionTypes)))
//                    .collect(toImmutableMap());
//        }
//
//        @Override
//        public void react(TableEvent event)
//        {
//            RecordCursor rc = new RecordCursorSymbolResolver(
//                    event.getAfter().get().getRecordCursor(),
//                    s -> event.getAfter().get().getLayout().get(s.getName()));
//            rc.advanceNextPosition();
//
//            for (Symbol s : expressionInterpreters.keySet()) {
//                expressionInterpreters.get(s).evaluate(rc);
//            }
//            destination.get().react(event);
//        }
//    }
//
//    public static class TableScanNodeReactor
//            extends InputNodeReactor<TableScanNode>
//    {
//        private final TableTupleLayout layout;
//
//        public TableScanNodeReactor(TableScanNode node, ReactorContext context, Optional<Reactor> destination)
//        {
//            super(node, context, destination);
//            layout = nodeLayout(node, tablePks(node.getTable(), node.getAssignments()));
//        }
//
//        @Override
//        public void react(TableEvent event)
//        {
//            destination.get().react(event);
//        }
//    }
//
//    public static class JoinNodeReactor
//            extends InnerNodeReactor<JoinNode>
//    {
//        public JoinNodeReactor(JoinNode node, ReactorContext context, Optional<Reactor> destination)
//        {
//            super(node, context, destination);
//            // pkl + pkr + (jk - hashes)
//        }
//
//        @Override
//        public void react(TableEvent event)
//        {
//
//        }
//    }
//
//    public static class ReactorPlanner
//    {
//        public ReactorContext run(ReactorContext reactorContext)
//        {
//            // FIXME make sure no indices used as they have no event sources .. or make them some
//            VisitorContext context = new VisitorContext(reactorContext);
//            Visitor visitor = new Visitor();
//            visitor.visitPlan(reactorContext.plan.getRoot(), context);
//            return reactorContext;
//        }
//
//        private static final class VisitorContext
//        {
//            private final ReactorContext reactorContext;
//
//            private final List<InputNodeReactor> inputReactors;
//            private final Optional<Reactor> destination;
//
//            public VisitorContext(ReactorContext reactorContext)
//            {
//                this.reactorContext = reactorContext;
//
//                inputReactors = newArrayList();
//                destination = Optional.empty();
//            }
//
//            public VisitorContext(VisitorContext parent, Reactor destination)
//            {
//                reactorContext = parent.reactorContext;
//
//                inputReactors = parent.inputReactors;
//                this.destination = Optional.of(destination);
//            }
//
//            public VisitorContext branch(Reactor destination)
//            {
//                return new VisitorContext(this, destination);
//            }
//        }
//
//        private class Visitor
//                extends PlanVisitor<ReactorPlanner.VisitorContext, Void>
//        {
//            private Reactor handleNode(PlanNode node, VisitorContext context)
//            {
//                Optional<Reactor> destination = context.destination;
//                if (node instanceof OutputNode) {
//                    return new OutputNodeReactor((OutputNode) node, context.reactorContext, destination);
//                }
//                else if (node instanceof ProjectNode) {
//                    return new ProjectNodeReactor((ProjectNode) node, context.reactorContext, destination);
//                }
//                else if (node instanceof TableScanNode) {
//                    return new TableScanNodeReactor((TableScanNode) node, context.reactorContext, destination);
//                }
//                else if (node instanceof JoinNode) {
//                    return new JoinNodeReactor((JoinNode) node, context.reactorContext, destination);
//                }
//                else {
//                    throw new UnsupportedPlanNodeException(node);
//                }
//            }
//
//            @Override
//            protected Void visitPlan(PlanNode node, VisitorContext context)
//            {
//                Reactor reactor = handleNode(node, context);
//                checkState(!context.reactorContext.reactors.containsKey(node.getId()));
//                context.reactorContext.reactors.put(node.getId(), reactor);
//                List<PlanNode> nodeSources = node.getSources();
//                if (!nodeSources.isEmpty()) {
//                    for (PlanNode source : node.getSources()) {
//                        source.accept(this, context.branch(reactor));
//                    }
//                }
//                else {
//                    checkState(reactor instanceof InputNodeReactor);
//                    context.inputReactors.add((InputNodeReactor) reactor);
//                }
//                return null;
//            }
//        }
//    }
//
//    @Test
//    public void testOtherShit()
//            throws Throwable
//    {
//        @Language("SQL") String sql =
//                //  "select * from tpch.tiny.\"orders\" as \"o\" " +
//                //  "inner join tpch.tiny.customer as \"c1\" on \"o\".custkey = \"c1\".custkey " +
//                //  "inner join tpch.tiny.customer as \"c2\" on \"o\".custkey = (\"c2\".custkey + 1)";
//
//                // "select o.custkey from tpch.tiny.\"orders\" as o where o.custkey in (select custkey from tpch.tiny.customer limit 10)";
//
//                // "create table test.test.foo as select o.custkey from tpch.tiny.\"orders\" as o where o.custkey in (select custkey from tpch.tiny.customer limit 10)";
//
//                // "insert into test.test.foo values (1)";
//
//                // "select custkey + 1 from tpch.tiny.\"orders\" as o where o.custkey in (1, 2, 3)";
//
//                // "select custkey + 1 \"custkey\", \"name\" from tpch.tiny.\"customer\"";
//
//                "select * from tpch.tiny.\"orders\" as \"o\" " +
//                "inner join tpch.tiny.customer as \"c\" on \"o\".custkey = \"c\".custkey ";
//
//        TestHelper.PlannedQuery pq = helper.plan(sql);
//
//        ReactorContext rc = new ReactorPlanner().run(
//                new ReactorContext(
//                        pq.plan,
//                        pq.analysis,
//                        pq.lqr.getDefaultSession(),
//                        pq.lqr.getMetadata(),
//                        pq.lqr.getConnectorManager().getConnectors(),
//                        pq.connectorSupport
//                )
//        );
//
//        for (InputNodeReactor s : rc.getInputReactors()) {
//            TableEvent e = new TableEvent(
//                    s,
//                    TableEvent.Operation.INSERT,
//                    Optional.<PkTableTuple>empty(),
//                    Optional.<PkTableTuple>of(
//                            new PkTableTuple(
//                                    new PkTableTupleLayout(ImmutableList.of("custkey", "name"), ImmutableList.of(BIGINT, VARCHAR), ImmutableList.of("custkey")),
//                                    ImmutableList.of(5, "phil")
//                            )
//                    )
//            );
//
//            s.react(e);
//        }
//    }
//}
