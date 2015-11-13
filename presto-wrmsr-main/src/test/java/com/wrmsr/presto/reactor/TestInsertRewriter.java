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
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.wrmsr.presto.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.jdbc.ExtendedJdbcConnector;
import com.wrmsr.presto.jdbc.ExtendedJdbcConnectorFactory;
import com.wrmsr.presto.util.ByteArrayWrapper;
import com.wrmsr.presto.util.Codecs;
import com.wrmsr.presto.util.Kv;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.ImmutableCollectors.toImmutableList;
import static com.wrmsr.presto.util.ImmutableCollectors.toImmutableMap;
import static com.wrmsr.presto.util.Maps.invertMap;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static java.util.Locale.ENGLISH;

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

    public static class Layout
    {
        private final List<String> names;
        private final List<Type> types;
        private final Optional<List<String>> pkNames;

        private final Map<String, Integer> indices;
        private final List<Integer> pkIndices;
        private final List<Integer> nonPkIndices;

        public Layout(List<String> names, List<Type> types, Optional<List<String>> pkNames)
        {
            checkArgument(names.size() == types.size());
            this.names = ImmutableList.copyOf(names);
            this.types = ImmutableList.copyOf(types);
            this.pkNames = pkNames.map(ImmutableList::copyOf);

            indices = IntStream.range(0, names.size()).boxed().map(i -> ImmutablePair.of(names.get(i), i)).collect(toImmutableMap());
            if (pkNames.isPresent()) {
                pkIndices = pkNames.get().stream().map(indices::get).collect(toImmutableList());
                Set<String> pkNameSet = ImmutableSet.copyOf(pkNames.get());
                nonPkIndices = IntStream.range(0, names.size()).boxed().filter(i -> !pkNameSet.contains(names.get(i))).collect(toImmutableList());
            }
            else {
                pkIndices = ImmutableList.of();
                nonPkIndices = IntStream.range(0, names.size()).boxed().collect(toImmutableList());
            }
        }

        public List<String> getNames()
        {
            return names;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public Optional<List<String>> getPkNames()
        {
            return pkNames;
        }

        public Map<String, Integer> getIndices()
        {
            return indices;
        }

        public List<Integer> getPkIndices()
        {
            checkState(pkNames.isPresent());
            return pkIndices;
        }

        public List<Integer> getNonPkIndices()
        {
            checkState(pkNames.isPresent());
            return nonPkIndices;
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
            Layout layout = (Layout) o;
            return Objects.equals(names, layout.names) &&
                    Objects.equals(types, layout.types) &&
                    Objects.equals(pkNames, layout.pkNames);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(names, types, pkNames);
        }
    }

    public class Tuple
    {
        private final Layout layout;
        private final List<Object> values;

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

        public List<Object> getPkValues()
        {
            return layout.getPkIndices().stream().map(values::get).collect(toImmutableList());
        }

        public List<Object> getNonPkValues()
        {
            return layout.getNonPkIndices().stream().map(values::get).collect(toImmutableList());
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
    }

    public static class TupleRecordCursor implements RecordCursor
    {
        @Override
        public long getTotalBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            return null;
        }

        @Override
        public boolean advanceNextPosition()
        {
            return false;
        }

        @Override
        public boolean getBoolean(int field)
        {
            return false;
        }

        @Override
        public long getLong(int field)
        {
            return 0;
        }

        @Override
        public double getDouble(int field)
        {
            return 0;
        }

        @Override
        public Slice getSlice(int field)
        {
            eek
            return null;
        }

        @Override
        public Object getObject(int field)
        {
            return null;
        }

        @Override
        public boolean isNull(int field)
        {
            return false;
        }

        @Override
        public void close()
        {

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
        private final Optional<Tuple> before;
        private final Optional<Tuple> after;

        public Event(Reactor source, Operation operation, Optional<Tuple> before, Optional<Tuple> after)
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

        public Optional<Tuple> getBefore()
        {
            return before;
        }

        public Optional<Tuple> getAfter()
        {
            return after;
        }
    }

    public static class ReactorContext
    {
        private final Plan plan;
        private final Analysis analysis;

        private final Map<String, Connector> connectors;
        private final Map<String, ConnectorSupport> connectorSupport;

        public ReactorContext(Plan plan, Analysis analysis, Map<String, Connector> connectors, Map<String, ConnectorSupport> connectorSupport)
        {
            this.plan = plan;
            this.analysis = analysis;
            this.connectors = connectors;
            this.connectorSupport = connectorSupport;
        }

        public Plan getPlan()
        {
            return plan;
        }

        public Analysis getAnalysis()
        {
            return analysis;
        }

        public Map<String, Connector> getConnectors()
        {
            return connectors;
        }

        public Map<String, ConnectorSupport> getConnectorSupport()
        {
            return connectorSupport;
        }

        public Kv<byte[], byte[]> getKv()
        {
            return new Kv.KeyCodec<>(new Kv.MapImpl<>(newHashMap()), ByteArrayWrapper.CODEC);
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

        protected static byte[] getKey(Tuple tuple)
        {
            checkArgument(tuple.getLayout().getPkNames().isPresent());
            try {
                return OBJECT_MAPPER.get().writeValueAsBytes(tuple.getPkValues());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        protected static byte[] getValue(Tuple tuple)
        {
            try {
                return OBJECT_MAPPER.get().writeValueAsBytes(tuple.getNonPkValues());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        protected Layout nodeLayout(PlanNode node, Optional<List<String>> pks)
        {
            Map<Symbol, Type> typeMap = context.plan.getSymbolAllocator().getTypes();
            List<String> names = node.getOutputSymbols().stream().map(s -> s.getName()).collect(toImmutableList());
            List<Type> types = names.stream().map(n -> typeMap.get(new Symbol(n))).collect(toImmutableList());
            return new Layout(names, types, pks);
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
        private final Layout layout;
        private final Kv<byte[], byte[]> kv;

        public OutputNodeReactor(OutputNode node, ReactorContext context, Optional<Reactor> destination)
        {
            super(node, context, destination);
            checkArgument(!destination.isPresent());
            layout = nodeLayout(node, Optional.<List<String>>empty());
            kv = context.getKv();
        }

        @Override
        public void react(Event event)
        {
            switch (event.getOperation()) {
                case INSERT: {
                    Tuple after = event.getAfter().get();
                    kv.put(getKey(after), getValue(after));
                    break;
                }
                case UPDATE: {
                    Tuple before = event.getBefore().get();
                    kv.remove(getKey(before));
                    Tuple after = event.getAfter().get();
                    kv.put(getKey(after), getValue(after));
                    break;
                }
                case DELETE: {
                    Tuple before = event.getBefore().get();
                    kv.remove(getKey(before));
                    break;
                }
            }
        }
    }

    public static class ProjectNodeReactor
            extends InnerNodeReactor<ProjectNode>
    {
        public ProjectNodeReactor(ProjectNode node, ReactorContext context, Optional<Reactor> destination)
        {
            super(node, context, destination);
        }

        @Override
        public void react(Event event)
        {
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
            layout = nodeLayout(node, Optional.of(tablePks(node.getTable(), node.getAssignments())));
        }

        @Override
        public void react(Event event)
        {
            destination.get().react(event);
        }
    }

    public static final class UnsupportedPlanNodeException
        extends RuntimeException
    {
        private final PlanNode node;

        public UnsupportedPlanNodeException(PlanNode node)
        {
            this.node = node;
        }

        @Override
        public String toString()
        {
            return "UnsupportedPlanNodeException{" +
                    "node=" + node +
                    '}';
        }
    }

    public static final class ReactorPlan
    {
        private final List<InputNodeReactor> sourceReactors;
        private final Map<PlanNode, Reactor> reactors;

        public ReactorPlan(List<InputNodeReactor> sourceReactors, Map<PlanNode, Reactor> reactors)
        {
            this.sourceReactors = sourceReactors;
            this.reactors = reactors;
        }

        public List<InputNodeReactor> getInputNodeReactors()
        {
            return sourceReactors;
        }

        public Map<PlanNode, Reactor> getReactors()
        {
            return reactors;
        }
    }

    public static class ReactorPlanner
    {
        public ReactorPlan run(ReactorContext reactorContext)
        {
            VisitorContext context = new VisitorContext(reactorContext);
            Visitor visitor = new Visitor();
            visitor.visitPlan(reactorContext.plan.getRoot(), context);
            return new ReactorPlan(
                    ImmutableList.copyOf(context.sourceHandlers),
                    ImmutableMap.copyOf(context.reactors)
            );
        }

        private static final class VisitorContext
        {
            private final ReactorContext reactorContext;

            private final List<InputNodeReactor> sourceHandlers;
            private final Map<PlanNode, Reactor> reactors;
            private final Optional<Reactor> destination;

            public VisitorContext(ReactorContext reactorContext)
            {
                this.reactorContext = reactorContext;

                sourceHandlers = newArrayList();
                reactors = newHashMap();
                destination = Optional.empty();
            }

            public VisitorContext(VisitorContext parent, Reactor destination)
            {
                reactorContext = parent.reactorContext;

                sourceHandlers = parent.sourceHandlers;
                reactors = parent.reactors;
                this.destination = Optional.of(destination);
            }

            public VisitorContext branch(Reactor destination)
            {
                return new VisitorContext(this, destination);
            }
        }

        private class Visitor extends PlanVisitor<ReactorPlanner.VisitorContext, Void>
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
                else {
                    throw new UnsupportedPlanNodeException(node);
                }
            }

            @Override
            protected Void visitPlan(PlanNode node, VisitorContext context)
            {
                Reactor reactor = handleNode(node, context);
                checkState(!context.reactors.containsKey(node));
                context.reactors.put(node, reactor);
                List<PlanNode> nodeSources = node.getSources();
                if (!nodeSources.isEmpty()) {
                    for (PlanNode source : node.getSources()) {
                        source.accept(this, context.branch(reactor));
                    }
                }
                else {
                    checkState(reactor instanceof InputNodeReactor);
                    context.sourceHandlers.add((InputNodeReactor) reactor);
                }
                return null;
            }
        }
    }

    public static abstract class ConnectorSupport<CF extends ConnectorFactory, C extends Connector>
    {
        private final Class<CF> connectorFactoryClass;
        private final Class<C> connectorClass;
        protected final C connector;

        @SuppressWarnings({"unchecked"})
        public ConnectorSupport(Class<CF> connectorFactoryClass, Class<C> connectorClass, C connector)
        {
            this.connectorFactoryClass = connectorFactoryClass;
            this.connectorClass = connectorClass;
            this.connector = (C) connector;
        }

        public abstract SchemaTableName getSchemaTableName(ConnectorTableHandle handle);

        public abstract List<String> getPrimaryKey(SchemaTableName schemaTableName);

        public abstract String getColumnName(ColumnHandle columnHandle);
    }

    public static class ExtendedJdbcConnectorSupport extends ConnectorSupport<ExtendedJdbcConnectorFactory, ExtendedJdbcConnector>
    {
        public ExtendedJdbcConnectorSupport(ExtendedJdbcConnector connector)
        {
            super(ExtendedJdbcConnectorFactory.class, ExtendedJdbcConnector.class, connector);
        }

        public ExtendedJdbcClient getClient()
        {
            return (ExtendedJdbcClient) connector.getJdbcClient();
        }

        @Override
        public SchemaTableName getSchemaTableName(ConnectorTableHandle handle)
        {
            checkArgument(handle instanceof JdbcTableHandle);
            return ((JdbcTableHandle) handle).getSchemaTableName();
        }

        @Override
        public List<String> getPrimaryKey(SchemaTableName schemaTableName)
        {
            try {
                try (Connection connection = getClient().getConnection()) {
                    DatabaseMetaData metadata = connection.getMetaData();

                    // FIXME postgres catalog support
                    try (ResultSet resultSet = metadata.getPrimaryKeys(schemaTableName.getSchemaName(), schemaTableName.getSchemaName(), schemaTableName.getTableName())) {
                        while (resultSet.next()) {
                            System.out.println(resultSet);
                        }
                    }

                    throw new UnsupportedOperationException();
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getColumnName(ColumnHandle columnHandle)
        {
            return ((JdbcColumnHandle) columnHandle).getColumnName();
        }
    }

    public static class TpchConnectorSupport extends ConnectorSupport<TpchConnectorFactory, Connector>
    {
        private final String defaultSchema;

        private static final Set<String> schemas = ImmutableSet.<String>builder()
                .add("tiny")
                .build();

        private static final Map<String, List<String>> tablePrimaryKeys = ImmutableMap.<String, List<String>>builder()
                .put("customer", ImmutableList.of("custkey"))
                .put("supplier", ImmutableList.of("suppkey"))
                .put("part", ImmutableList.of("partkey"))
                .put("nation", ImmutableList.of("nationkey"))
                .put("region", ImmutableList.of("regionkey"))
                .put("partsupp", ImmutableList.of("partkey", "suppkey"))
                .put("orders", ImmutableList.of("orderkey"))
                .build();

        public TpchConnectorSupport(Connector connector, String defaultSchema)
        {
            super(TpchConnectorFactory.class, Connector.class, connector);
            checkArgument(schemas.contains(defaultSchema));
            this.defaultSchema = defaultSchema;
        }

        @Override
        public SchemaTableName getSchemaTableName(ConnectorTableHandle handle)
        {
            checkArgument(handle instanceof TpchTableHandle);
            return new SchemaTableName(defaultSchema, ((TpchTableHandle) handle).getTableName());
        }

        @Override
        public List<String> getPrimaryKey(SchemaTableName schemaTableName)
        {
            checkArgument(schemas.contains(schemaTableName.getSchemaName()));
            checkArgument(tablePrimaryKeys.containsKey(schemaTableName.getTableName()));
            return tablePrimaryKeys.get(schemaTableName.getTableName());
        }

        @Override
        public String getColumnName(ColumnHandle columnHandle)
        {
            return ((TpchColumnHandle) columnHandle).getColumnName();
        }
    }

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
                TestInsertRewriter.class.getClassLoader());

        localQueryRunner.createCatalog("test", connectorFactory, ImmutableMap.<String, String>of());

        Connector connector = connectorFactory.create("test", TestingH2JdbcModule.createProperties(db));
        ConnectorMetadata metadata = connector.getMetadata();
        JdbcMetadata jdbcMetadata = (JdbcMetadata) metadata;
        BaseJdbcClient jdbcClient = (BaseJdbcClient) jdbcMetadata.getJdbcClient();
        try {
            try (Connection connection = jdbcClient.getConnection()) {
                try (java.sql.Statement stmt = connection.createStatement()) {
                    stmt.execute("CREATE SCHEMA test");
                    stmt.execute("CREATE TABLE test.foo (id integer primary key)");
                }
                // connection.createStatement().execute("CREATE TABLE example.foo (id integer primary key)");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


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
    public void testOtherShit()
            throws Throwable
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
            @Language("SQL") String sql =
                    //  "select * from tpch.tiny.\"orders\" as \"o\" " +
                    //  "inner join tpch.tiny.customer as \"c1\" on \"o\".custkey = \"c1\".custkey " +
                    //  "inner join tpch.tiny.customer as \"c2\" on \"o\".custkey = (\"c2\".custkey + 1)";

                    // "select o.custkey from tpch.tiny.\"orders\" as o where o.custkey in (select custkey from tpch.tiny.customer limit 10)";

                    // "create table test.test.foo as select o.custkey from tpch.tiny.\"orders\" as o where o.custkey in (select custkey from tpch.tiny.customer limit 10)";

                    // "insert into test.test.foo values (1)";

                    // "select custkey + 1 from tpch.tiny.\"orders\" as o where o.custkey in (1, 2, 3)";

                    "select custkey + 1 from tpch.tiny.\"customer\"";

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

            Connector tpchConn = lqr.getConnectorManager().getConnectors().get("tpch");
            List<String> pks = new TpchConnectorSupport(tpchConn, "tiny").getPrimaryKey(new SchemaTableName("tiny", "customer"));

            ReactorContext rc = new ReactorContext(
                    plan,
                    analysis,
                    lqr.getConnectorManager().getConnectors(),
                    ImmutableMap.<String, ConnectorSupport>builder()
                            .put("tpch", new TpchConnectorSupport(tpchConn, "tiny"))
                            .build()
            );
            ReactorPlan rp = new ReactorPlanner().run(rc);
            for (InputNodeReactor s : rp.getInputNodeReactors()) {
                Event e = new Event(
                        s,
                        Event.Operation.INSERT,
                        Optional.<Tuple>empty(),
                        Optional.<Tuple>of(
                                new Tuple(
                                        new Layout(ImmutableList.of("custkey"), ImmutableList.of(BIGINT), Optional.of(ImmutableList.of("custkey"))),
                                        ImmutableList.of(5)
                                )));
                s.react(e);
                // rc.getConnectorSupport().get(s.getNode().)

            }

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
    public void testShit()
            throws Throwable
    {
        @Language("SQL") String queryStr = "select a, b from t1";

        Statement statement = SQL_PARSER.createStatement(queryStr);
        Analysis analysis = analyzer.analyze(statement);

        InsertRewriter rewriter = new InsertRewriter(analysis);
        rewriter.process(statement, null);

        Field pk = analysis.getOutputDescriptor().getFieldByIndex(0);
        Query query = analysis.getQuery();
        QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
        Table table = (Table) querySpecification.getFrom().get();

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
