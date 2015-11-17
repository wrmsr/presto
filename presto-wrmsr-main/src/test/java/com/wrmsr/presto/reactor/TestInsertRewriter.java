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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.wrmsr.presto.util.ByteArrayWrapper;
import com.wrmsr.presto.util.collect.fuuu;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
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

public class TestInsertRewriter
{
    private final TestHelper helper = new TestHelper();

    public static class Layout
    {
        protected final List<String> names;
        protected final List<Type> types;
        protected final Map<String, Integer> indices;

        public Layout(List<String> names, List<Type> types)
        {
            checkArgument(names.size() == types.size());
            this.names = ImmutableList.copyOf(names);
            this.types = ImmutableList.copyOf(types);
            indices = IntStream.range(0, names.size()).boxed().map(i -> ImmutablePair.of(names.get(i), i)).collect(toImmutableMap());
        }

        public List<String> getNames()
        {
            return names;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public Map<String, Integer> getIndices()
        {
            return indices;
        }

        public int get(String name)
        {
            return indices.get(name);
        }

        @Override
        public String toString()
        {
            return "Layout{" +
                    "names=" + names +
                    ", types=" + types +
                    '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Layout layout = (Layout) o;
            return Objects.equals(names, layout.names) &&
                    Objects.equals(types, layout.types);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(names, types);
        }
    }

    public static class PkLayout
            extends Layout
    {
        private final List<String> pkNames;

        protected final List<String> nonPkNames;
        protected final List<Integer> pkIndices;
        protected final List<Integer> nonPkIndices;
        protected final List<Type> pkTypes;
        protected final List<Type> nonPkTypes;

        protected final Layout pk;
        protected final Layout nonPk;

        public PkLayout(List<String> names, List<Type> types, List<String> pkNames)
        {
            super(names, types);
            checkArgument(names.size() == types.size());
            this.pkNames = ImmutableList.copyOf(pkNames);

            Set<String> pkNameSet = ImmutableSet.copyOf(pkNames);
            nonPkNames = names.stream().filter(n -> !pkNameSet.contains(n)).collect(toImmutableList());
            pkIndices = pkNames.stream().map(indices::get).collect(toImmutableList());
            nonPkIndices = IntStream.range(0, names.size()).boxed().filter(i -> !pkNameSet.contains(names.get(i))).collect(toImmutableList());
            pkTypes = pkIndices.stream().map(types::get).collect(toImmutableList());
            nonPkTypes = nonPkIndices.stream().map(types::get).collect(toImmutableList());

            pk = new Layout(pkNames, pkTypes);
            nonPk = new Layout(nonPkNames, nonPkTypes);
        }

        public List<String> getPkNames()
        {
            return pkNames;
        }

        public List<String> getNonPkNames()
        {
            return nonPkNames;
        }

        public List<Integer> getPkIndices()
        {
            return pkIndices;
        }

        public List<Integer> getNonPkIndices()
        {
            return nonPkIndices;
        }

        public List<Type> getPkTypes()
        {
            return pkTypes;
        }

        public List<Type> getNonPkTypes()
        {
            return nonPkTypes;
        }

        public Layout getPk()
        {
            return pk;
        }

        public Layout getNonPk()
        {
            return nonPk;
        }

        @Override
        public String toString()
        {
            return "PkLayout{" +
                    "names=" + names +
                    ", types=" + types +
                    ", pkNames=" + pkNames +
                    '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            PkLayout pkLayout = (PkLayout) o;
            return Objects.equals(pkNames, pkLayout.pkNames);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), pkNames);
        }
    }

    public static class Tuple
    {
        protected final Layout layout;
        protected final List<Object> values;

        public Tuple(Layout layout, List<Object> values)
        {
            this.layout = layout;
            this.values = ImmutableList.copyOf(values);
        }

        public Layout getLayout()
        {
            return layout;
        }

        public List<Object> getValues()
        {
            return values;
        }

        @Override
        public String toString()
        {
            return "Tuple{" +
                    "layout=" + layout +
                    ", values=" + values +
                    '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Tuple tuple = (Tuple) o;
            return Objects.equals(layout, tuple.layout) &&
                    Objects.equals(values, tuple.values);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(layout, values);
        }

        public RecordSet getRecordSet()
        {
            return new InMemoryRecordSet(layout.getTypes(), ImmutableList.of(values));
        }

        public RecordCursor getRecordCursor()
        {
            return getRecordSet().cursor();
        }

        public static Block toBlock(Layout layout, List<Object> values)
        {
            BlockBuilder b = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 10000);
            for (int i = 0; i < values.size(); ++i) {
                b.write(layout.getTypes().get(i), values.get(i));
            }
            return b.build();
        }

        public static Slice toSlice(Layout layout, List<Object> values, BlockEncodingSerde blockEncodingSerde)
        {
            Block block = toBlock(layout, values);
            SliceOutput output = new DynamicSliceOutput(64);
            BlockEncoding encoding = block.getEncoding();
            blockEncodingSerde.writeBlockEncoding(output, encoding);
            encoding.writeBlock(output, block);
            return output.slice();
        }

        public Block toBlock()
        {
            return toBlock(layout, values);
        }

        public Slice toSlice(BlockEncodingSerde blockEncodingSerde)
        {
            return toSlice(layout, values, blockEncodingSerde);
        }

        // TODO: terse jackson serializer too for pk

        public byte[] toBytes(BlockEncodingSerde blockEncodingSerde)
        {
            return toSlice(blockEncodingSerde).getBytes();
        }

        public static Tuple fromBlock(Layout layout, Block block)
        {
            ImmutableList.Builder<Object> builder = ImmutableList.builder();
            for (int i = 0; i < layout.getTypes().size(); ++i) {
                builder.add(block.read(layout.getTypes().get(i), i));
            }
            return new Tuple(layout, builder.build());
        }

        public static Tuple fromSlice(Layout layout, Slice slice, BlockEncodingSerde blockEncodingSerde)
        {
            BasicSliceInput input = slice.getInput();
            BlockEncoding blockEncoding = blockEncodingSerde.readBlockEncoding(input);
            return fromBlock(layout, blockEncoding.readBlock(input));
        }

        public static Tuple fromBytes(Layout layout, byte[] bytes, BlockEncodingSerde blockEncodingSerde)
        {
            return fromSlice(layout, Slices.wrappedBuffer(bytes), blockEncodingSerde);
        }
    }

    public static class PkTuple
            extends Tuple
    {
        public PkTuple(PkLayout layout, List<Object> values)
        {
            super(layout, values);
        }

        public PkLayout getPkLayout()
        {
            return (PkLayout) layout;
        }

        public List<Object> getPkValues()
        {
            return getPkLayout().getPkIndices().stream().map(values::get).collect(toImmutableList());
        }

        public List<Object> getNonPkValues()
        {
            return getPkLayout().getNonPkIndices().stream().map(values::get).collect(toImmutableList());
        }

        public Tuple getPk()
        {
            return new Tuple(getPkLayout().getPk(), getPkValues());
        }

        public Tuple getNonPk()
        {
            return new Tuple(getPkLayout().getNonPk(), getNonPkValues());
        }
    }

    public static class Event
    {
        public enum Operation
        {
            INSERT,
            UPDATE,
            DELETE
        }

        private final Reactor source;
        private final Operation operation;
        private final Optional<PkTuple> before;
        private final Optional<PkTuple> after;

        public Event(Reactor source, Operation operation, Optional<PkTuple> before, Optional<PkTuple> after)
        {
            this.source = source;
            this.operation = operation;
            this.before = before;
            this.after = after;
        }

        public Event(Event parent, Reactor source)
        {
            this.source = source;
            operation = parent.operation;
            before = parent.before;
            after = parent.after;
        }

        public Event branch(Reactor source)
        {
            return new Event(this, source);
        }

        public Reactor getSource()
        {
            return source;
        }

        public Operation getOperation()
        {
            return operation;
        }

        public Optional<PkTuple> getBefore()
        {
            return before;
        }

        public Optional<PkTuple> getAfter()
        {
            return after;
        }
    }

    public static class ReactorContext
    {
        private final Plan plan;
        private final Analysis analysis;
        private final Session sesion;
        private final Metadata metadata;

        private final Map<String, Connector> connectors;
        private final Map<String, ConnectorSupport> connectorSupport;

        private final Map<PlanNodeId, Reactor> reactors;

        public ReactorContext(Plan plan, Analysis analysis, Session session, Metadata metadata, Map<String, Connector> connectors, Map<String, ConnectorSupport> connectorSupport)
        {
            this.plan = plan;
            this.analysis = analysis;
            this.sesion = session;
            this.metadata = metadata;
            this.connectors = connectors;
            this.connectorSupport = connectorSupport;
            reactors = newHashMap();
        }

        public Plan getPlan()
        {
            return plan;
        }

        public Analysis getAnalysis()
        {
            return analysis;
        }

        public Session getSesion()
        {
            return sesion;
        }

        public Metadata getMetadata()
        {
            return metadata;
        }

        public BlockEncodingSerde getBlockEncodingSerde()
        {
            return metadata.getBlockEncodingSerde();
        }

        public Map<String, Connector> getConnectors()
        {
            return connectors;
        }

        public Map<String, ConnectorSupport> getConnectorSupport()
        {
            return connectorSupport;
        }

        public Map<PlanNodeId, Reactor> getReactors()
        {
            return reactors;
        }

        public List<InputNodeReactor> getInputReactors()
        {
            return reactors.values().stream().filter(InputNodeReactor.class::isInstance).map(InputNodeReactor.class::cast).collect(toImmutableList());
        }

        public fuuu<byte[], byte[]> getKv()
        {
            return new fuuu.KeyCodec<>(new fuuu.MapImpl<>(newHashMap()), ByteArrayWrapper.CODEC);
        }
    }

    public static abstract class Reactor<T extends PlanNode>
    {
        protected final T node;
        protected final ReactorContext context;
        protected final Optional<Reactor> destination;

        public Reactor(T node, ReactorContext context, Optional<Reactor> destination)
        {
            this.node = node;
            this.context = context;
            this.destination = destination;
        }

        public abstract void react(Event event);

        protected Layout nodeLayout(PlanNode node)
        {
            Map<Symbol, Type> typeMap = context.plan.getSymbolAllocator().getTypes();
            List<String> names = node.getOutputSymbols().stream().map(s -> s.getName()).collect(toImmutableList());
            List<Type> types = names.stream().map(n -> typeMap.get(new Symbol(n))).collect(toImmutableList());
            return new Layout(names, types);
        }

        protected PkLayout nodeLayout(PlanNode node, List<String> pks)
        {
            Layout layout = nodeLayout(node);
            return new PkLayout(layout.getNames(), layout.getTypes(), pks);
        }

        protected List<String> tablePkCols(TableHandle th)
        {
            ConnectorSupport cs = context.getConnectorSupport().get(th.getConnectorId());
            SchemaTableName stn = cs.getSchemaTableName(th.getConnectorHandle());
            return cs.getPrimaryKey(stn);
        }

        protected List<String> tablePks(TableHandle th, Map<Symbol, ColumnHandle> assignments)
        {
            ConnectorSupport cs = context.connectorSupport.get(th.getConnectorId());
            List<String> pkCols = tablePkCols(th);
            Map<String, Symbol> m = assignments.entrySet().stream().map(e -> ImmutablePair.of(cs.getColumnName(e.getValue()), e.getKey())).collect(toImmutableMap());
            return pkCols.stream().map(c -> m.get(c).getName()).collect(toImmutableList());
        }

        public List<Reactor> getSources()
        {
            return node.getSources().stream().map(n -> context.reactors.get(n.getId())).collect(toImmutableList());
        }

        public Reactor getSource()
        {
            List<Reactor> sources = getSources();
            checkState(sources.size() == 1);
            return sources.get(0);
        }
    }

    public static abstract class InputNodeReactor<T extends PlanNode>
            extends Reactor<T>
    {
        public InputNodeReactor(T node, ReactorContext context, Optional<Reactor> destination)
        {
            super(node, context, destination);
            checkArgument(destination.isPresent());
        }
    }

    public static abstract class InnerNodeReactor<T extends PlanNode>
            extends Reactor<T>
    {
        public InnerNodeReactor(T node, ReactorContext context, Optional<Reactor> destination)
        {
            super(node, context, destination);
            checkArgument(destination.isPresent());
        }
    }

    public static class OutputNodeReactor
            extends Reactor<OutputNode>
    {
        // private final Layout layout;
        private final fuuu<byte[], byte[]> SImpleMap;

        public OutputNodeReactor(OutputNode node, ReactorContext context, Optional<Reactor> destination)
        {
            super(node, context, destination);
            checkArgument(!destination.isPresent());
            // layout = nodeLayout(node);
            SImpleMap = context.getKv(); }

        @Override
        public void react(Event event)
        {
            BlockEncodingSerde bes = context.getBlockEncodingSerde();
            switch (event.getOperation()) {
                case INSERT: {
                    PkTuple after = event.getAfter().get();
                    SImpleMap.put(after.getPk().toBytes(bes), after.getNonPk().toBytes(bes));
                    break;
                }
                case UPDATE: {
                    PkTuple before = event.getBefore().get();
                    SImpleMap.remove(before.getPk().toBytes(bes));
                    PkTuple after = event.getAfter().get();
                    SImpleMap.put(after.getPk().toBytes(bes), after.getNonPk().toBytes(bes));
                    break;
                }
                case DELETE: {
                    PkTuple before = event.getBefore().get();
                    SImpleMap.remove(before.toBytes(bes));
                    break;
                }
            }
        }
    }

    @FunctionalInterface
    public interface SymbolIndexResolver
    {
        int getSymbolIndex(Symbol symbol);
    }

    public static class RecordCursorSymbolResolver
            implements RecordCursor, SymbolResolver
    {
        private final RecordCursor recordCursor;
        private final SymbolIndexResolver symbolIndexResolver;

        public RecordCursorSymbolResolver(RecordCursor recordCursor, SymbolIndexResolver symbolIndexResolver)
        {
            this.recordCursor = recordCursor;
            this.symbolIndexResolver = symbolIndexResolver;
        }

        @Override
        public Object getValue(Symbol symbol)
        {
            return getObject(symbolIndexResolver.getSymbolIndex(symbol));
        }

        @Override
        public long getTotalBytes()
        {
            return recordCursor.getTotalBytes();
        }

        @Override
        public long getCompletedBytes()
        {
            return recordCursor.getCompletedBytes();
        }

        @Override
        public long getReadTimeNanos()
        {
            return recordCursor.getReadTimeNanos();
        }

        @Override
        public Type getType(int field)
        {
            return recordCursor.getType(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            return recordCursor.advanceNextPosition();
        }

        @Override
        public boolean getBoolean(int field)
        {
            return recordCursor.getBoolean(field);
        }

        @Override
        public long getLong(int field)
        {
            return recordCursor.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            return recordCursor.getDouble(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            return recordCursor.getSlice(field);
        }

        @Override
        public Object getObject(int field)
        {
            return recordCursor.getObject(field);
        }

        @Override
        public boolean isNull(int field)
        {
            return recordCursor.isNull(field);
        }

        @Override
        public void close()
        {
            recordCursor.close();
        }
    }

    public static class ProjectNodeReactor
            extends InnerNodeReactor<ProjectNode>
    {
        private final Map<Symbol, ExpressionInterpreter> expressionInterpreters;

        public ProjectNodeReactor(ProjectNode node, ReactorContext context, Optional<Reactor> destination)
        {
            super(node, context, destination);

            Map<Expression, Type> expressionTypes = ImmutableMap.copyOf(context.getAnalysis().getTypes());
            expressionInterpreters = node.getAssignments().entrySet().stream()
                    .map(e -> ImmutablePair.of(
                            e.getKey(),
                            // FIXME: duplicate names?
                            ExpressionInterpreter.expressionInterpreterUnsafe(
                                    e.getValue(),
                                    context.getMetadata(),
                                    context.getSesion(),
                                    expressionTypes)))
                    .collect(toImmutableMap());
        }

        @Override
        public void react(Event event)
        {
            RecordCursor rc = new RecordCursorSymbolResolver(
                    event.after.get().getRecordCursor(),
                    s -> event.after.get().getLayout().get(s.getName()));
            rc.advanceNextPosition();

            for (Symbol s : expressionInterpreters.keySet()) {
                expressionInterpreters.get(s).evaluate(rc);
            }
            destination.get().react(event);
        }
    }

    public static class TableScanNodeReactor
            extends InputNodeReactor<TableScanNode>
    {
        private final Layout layout;

        public TableScanNodeReactor(TableScanNode node, ReactorContext context, Optional<Reactor> destination)
        {
            super(node, context, destination);
            layout = nodeLayout(node, tablePks(node.getTable(), node.getAssignments()));
        }

        @Override
        public void react(Event event)
        {
            destination.get().react(event);
        }
    }

    public static class JoinNodeReactor
            extends InnerNodeReactor<JoinNode>
    {
        public JoinNodeReactor(JoinNode node, ReactorContext context, Optional<Reactor> destination)
        {
            super(node, context, destination);
            // pkl + pkr + (jk - hashes)
        }

        @Override
        public void react(Event event)
        {

        }
    }

    public static class ReactorPlanner
    {
        public ReactorContext run(ReactorContext reactorContext)
        {
            // FIXME make sure no indices used as they have no event sources .. or make them some
            VisitorContext context = new VisitorContext(reactorContext);
            Visitor visitor = new Visitor();
            visitor.visitPlan(reactorContext.plan.getRoot(), context);
            return reactorContext;
        }

        private static final class VisitorContext
        {
            private final ReactorContext reactorContext;

            private final List<InputNodeReactor> inputReactors;
            private final Optional<Reactor> destination;

            public VisitorContext(ReactorContext reactorContext)
            {
                this.reactorContext = reactorContext;

                inputReactors = newArrayList();
                destination = Optional.empty();
            }

            public VisitorContext(VisitorContext parent, Reactor destination)
            {
                reactorContext = parent.reactorContext;

                inputReactors = parent.inputReactors;
                this.destination = Optional.of(destination);
            }

            public VisitorContext branch(Reactor destination)
            {
                return new VisitorContext(this, destination);
            }
        }

        private class Visitor
                extends PlanVisitor<ReactorPlanner.VisitorContext, Void>
        {
            private Reactor handleNode(PlanNode node, VisitorContext context)
            {
                Optional<Reactor> destination = context.destination;
                if (node instanceof OutputNode) {
                    return new OutputNodeReactor((OutputNode) node, context.reactorContext, destination);
                }
                else if (node instanceof ProjectNode) {
                    return new ProjectNodeReactor((ProjectNode) node, context.reactorContext, destination);
                }
                else if (node instanceof TableScanNode) {
                    return new TableScanNodeReactor((TableScanNode) node, context.reactorContext, destination);
                }
                else if (node instanceof JoinNode) {
                    return new JoinNodeReactor((JoinNode) node, context.reactorContext, destination);
                }
                else {
                    throw new UnsupportedPlanNodeException(node);
                }
            }

            @Override
            protected Void visitPlan(PlanNode node, VisitorContext context)
            {
                Reactor reactor = handleNode(node, context);
                checkState(!context.reactorContext.reactors.containsKey(node.getId()));
                context.reactorContext.reactors.put(node.getId(), reactor);
                List<PlanNode> nodeSources = node.getSources();
                if (!nodeSources.isEmpty()) {
                    for (PlanNode source : node.getSources()) {
                        source.accept(this, context.branch(reactor));
                    }
                }
                else {
                    checkState(reactor instanceof InputNodeReactor);
                    context.inputReactors.add((InputNodeReactor) reactor);
                }
                return null;
            }
        }
    }

    @Test
    public void testOtherShit()
            throws Throwable
    {
        @Language("SQL") String sql =
                //  "select * from tpch.tiny.\"orders\" as \"o\" " +
                //  "inner join tpch.tiny.customer as \"c1\" on \"o\".custkey = \"c1\".custkey " +
                //  "inner join tpch.tiny.customer as \"c2\" on \"o\".custkey = (\"c2\".custkey + 1)";

                // "select o.custkey from tpch.tiny.\"orders\" as o where o.custkey in (select custkey from tpch.tiny.customer limit 10)";

                // "create table test.test.foo as select o.custkey from tpch.tiny.\"orders\" as o where o.custkey in (select custkey from tpch.tiny.customer limit 10)";

                // "insert into test.test.foo values (1)";

                // "select custkey + 1 from tpch.tiny.\"orders\" as o where o.custkey in (1, 2, 3)";

                // "select custkey + 1 \"custkey\", \"name\" from tpch.tiny.\"customer\"";

                "select * from tpch.tiny.\"orders\" as \"o\" " +
                "inner join tpch.tiny.customer as \"c\" on \"o\".custkey = \"c\".custkey ";

        TestHelper.PlannedQuery pq = helper.plan(sql);

        ReactorContext rc = new ReactorPlanner().run(
                new ReactorContext(
                        pq.plan,
                        pq.analysis,
                        pq.lqr.getDefaultSession(),
                        pq.lqr.getMetadata(),
                        pq.lqr.getConnectorManager().getConnectors(),
                        pq.connectorSupport
                )
        );

        for (InputNodeReactor s : rc.getInputReactors()) {
            Event e = new Event(
                    s,
                    Event.Operation.INSERT,
                    Optional.<PkTuple>empty(),
                    Optional.<PkTuple>of(
                            new PkTuple(
                                    new PkLayout(ImmutableList.of("custkey", "name"), ImmutableList.of(BIGINT, VARCHAR), ImmutableList.of("custkey")),
                                    ImmutableList.of(5, "phil")
                            )
                    )
            );

            s.react(e);
        }
    }
}
