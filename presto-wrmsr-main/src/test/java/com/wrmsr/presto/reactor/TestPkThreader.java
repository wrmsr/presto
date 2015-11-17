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
n-way join plz, no hash opt
 - just manually add lol
*/

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
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
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableSet;
import static jersey.repackaged.com.google.common.collect.Lists.asList;
import static jersey.repackaged.com.google.common.collect.Lists.newArrayList;

public class TestPkThreader
{
    public final TestHelper helper = new TestHelper();

    @FunctionalInterface
    public interface IntermediateStorageProvider
    {
        final class Column
        {
            private final String name;
            private final Type type;

            public Column(String name, Type type)
            {
                this.name = name;
                this.type = type;
            }

            public String getName()
            {
                return name;
            }

            public Type getType()
            {
                return type;
            }
        }

        TableHandle getIntermediateStorage(String name, List<Column> pkColumns, List<Column> nonPkColumns);
    }

    public static class PkThreader extends PlanVisitor<PkThreader.Context, PlanNode>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Session session;
        private final Metadata metadata;
        private final Map<String, ConnectorSupport> connectorSupport;
        private final IntermediateStorageProvider intermediateStorageProvider;

        public PkThreader(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session, Metadata metadata, Map<String, ConnectorSupport> connectorSupport, IntermediateStorageProvider intermediateStorageProvider)
        {
            this.idAllocator = idAllocator;
            this.symbolAllocator = symbolAllocator;
            this.session = session;
            this.metadata = metadata;
            this.connectorSupport = connectorSupport;
            this.intermediateStorageProvider = intermediateStorageProvider;
        }

        public static class Context
        {
            private final Map<PlanNodeId, List<Symbol>> nodePkSyms;
            private final List<Context> children;

            public Context()
            {
                nodePkSyms = newHashMap();
                children = newArrayList();
            }
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Context context)
        {
            throw new UnsupportedPlanNodeException(node);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Context context)
        {
            PlanNode newSource = node.getSource().accept(this, context);
            List<Symbol> pkSyms = context.nodePkSyms.get(newSource.getId());

            Set<QualifiedNameReference> pkQnrs = pkSyms.stream().map(Symbol::toQualifiedNameReference).collect(toImmutableSet());
            Set<Symbol> identityAssignments = node.getAssignments().entrySet().stream().filter(e -> pkQnrs.contains(e.getValue())).map(Map.Entry::getKey).collect(toImmutableSet());

            Map<Symbol, Expression> newAssignments = newHashMap(node.getAssignments());
            for (Symbol pkSym : pkSyms) {
                if (!identityAssignments.contains(pkSym)) {
                    newAssignments.put(pkSym, pkSym.toQualifiedNameReference());
                }
            }

            ProjectNode newNode = new ProjectNode(
                node.getId(),
                newSource,
                newAssignments);
            context.nodePkSyms.put(newNode.getId(), pkSyms);
            return newNode;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Context context)
        {
            PlanNode newSource = node.getSource().accept(this, context);
            List<Symbol> pkSyms = context.nodePkSyms.get(newSource.getId());

            FilterNode newNode = new FilterNode(
                    node.getId(),
                    node.getSource(),
                    node.getPredicate());
            context.nodePkSyms.put(newNode.getId(), pkSyms);
            return newNode;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, Context context)
        {
            PlanNode newLeft = node.getLeft().accept(this, context);
            List<Symbol> leftPkSyms = context.nodePkSyms.get(newLeft.getId());

            PlanNode newRight = node.getRight().accept(this, context);
            List<Symbol> rightPkSyms = context.nodePkSyms.get(newRight.getId());

            List<Symbol> pkSyms = ImmutableList.<Symbol>builder()
                    .addAll(leftPkSyms)
                    .addAll(rightPkSyms)
                    .build();
            checkState(pkSyms.size() == newHashSet(pkSyms).size());

            JoinNode newNode = new JoinNode(
                    node.getId(),
                    node.getType(),
                    newLeft,
                    newRight,
                    node.getCriteria(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol()
            );
            context.nodePkSyms.put(newNode.getId(), pkSyms);
            return newNode;
        }

        @Override
        public PlanNode visitOutput(OutputNode node, Context context)
        {
            PlanNode newSource = node.getSource().accept(this, context);
            List<Symbol> pkSyms = context.nodePkSyms.get(newSource.getId());

            Set<Symbol> outputSymbolSet = newHashSet(node.getOutputSymbols());

            List<String> newColumnNames = newArrayList(node.getColumnNames());
            List<Symbol> newOutputSymbols = newArrayList(node.getOutputSymbols());

            for (Symbol pkSym : pkSyms) {
                if (!outputSymbolSet.contains(pkSym)) {
                    String pkCol = pkSym.getName();
                    checkState(!newColumnNames.contains(pkCol));
                    newColumnNames.add(pkCol);
                    newOutputSymbols.add(pkSym);
                }
            }

            OutputNode newNode = new OutputNode(
                    node.getId(),
                    newSource,
                    newColumnNames,
                    newOutputSymbols);
            context.nodePkSyms.put(newNode.getId(), pkSyms);
            return newNode;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Context context)
        {
            // FIXME (optional?) filter extraction - we (may?) only get deltas
            ConnectorSupport cs = connectorSupport.get(node.getTable().getConnectorId());
            Connector c = cs.getConnector();
            ConnectorSession csess = session.toConnectorSession();
            SchemaTableName stn = cs.getSchemaTableName(node.getTable().getConnectorHandle());
            ConnectorTableHandle th = c.getMetadata().getTableHandle(csess, stn);
            Map<String, ColumnHandle> chs = c.getMetadata().getColumnHandles(csess, th);

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
            context.nodePkSyms.put(newNode.getId(), pkSyms);
            return newNode;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, Context context)
        {
            Context backingContext = new Context();
            PlanNode backingSource = node.getSource().accept(this, backingContext);
            context.children.add(backingContext);
            List<Symbol> backingPkSyms = backingContext.nodePkSyms.get(backingSource.getId());

            List<Symbol> gkSyms = node.getGroupBy();
            Set<Symbol> gkSymSet = newHashSet(gkSyms);
            List<Symbol> nonGkSyms = node.getOutputSymbols().stream().filter(s -> !gkSymSet.contains(s)).collect(toImmutableList());

            Function<List<Symbol>, List<IntermediateStorageProvider.Column>> toIspCols = ss ->
                    ss.stream().map(s -> new IntermediateStorageProvider.Column(s.getName(), symbolAllocator.getTypes().get(s))).collect(toImmutableList());

            TableHandle dataTableHandle = intermediateStorageProvider.getIntermediateStorage(
                    String.format("%s_data", node.getId().toString()),
                    toIspCols.apply(gkSyms),
                    toIspCols.apply(nonGkSyms));

            TableMetadata dataTableMetadata = metadata.getTableMetadata(session, dataTableHandle);
            ConnectorSupport dataTableConnectorSupport = connectorSupport.get(dataTableHandle.getConnectorId());
            Connector dataTableConnector = dataTableConnectorSupport.getConnector();
            Map<String, ColumnHandle> dataTableColumnHandles = dataTableConnector.getMetadata().getColumnHandles(session.toConnectorSession(), dataTableHandle.getConnectorHandle());

            Map<Symbol, ColumnHandle> newAssignments = node.getOutputSymbols().stream().map(s -> ImmutablePair.of(s, dataTableColumnHandles.get(s.getName()))).collect(toImmutableMap());

            /*
            TableScanNode newNode = new TableScanNode(
                idAllocator.getNextId(),
                dataTableHandle,
                node.getOutputSymbols(),
                newAssignments,
                Optional.<TableLayoutHandle>empty(),
                TupleDomain<ColumnHandle> currentConstraint,
                null)

            AggregationNode newNode = new AggregationNode(
                    node.getId(),
                    node.getSource(),
                    node.getGroupBy(),
                    node.getAggregations(),
                    node.getFunctions(),
                    node.getMasks()
                    node.getSampleWeight(),
                    node.getConfidence());
            return newNode;
            */
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void testThing()
            throws Throwable
    {
        @Language("SQL") String stmt =
                // "select customer.name, nation.name from tpch.tiny.customer inner join tpch.tiny.nation on customer.nationkey = nation.nationkey where acctbal > 100"
                "select nationkey, count(*) c from tpch.tiny.nation group by nationkey"
                ;

        TestHelper.PlannedQuery pq = helper.plan(stmt);
        PkThreader.Context ctx = new PkThreader.Context();

        Function<Type, String> formatSqlCol = t -> {
            if (t instanceof BigintType) {
                return "bigint";
            }
            else {
                throw new UnsupportedOperationException();
            }
        };

        IntermediateStorageProvider isp = (n, gkcs, ngkcs) -> {
            StringBuilder sql = new StringBuilder();
            sql.append("create table " + n + " (");
            sql.append(Stream.concat(gkcs.stream(), ngkcs.stream())
                    .map(c -> String.format("'%s' %s", c.getName(), formatSqlCol.apply(c.getType())))
                    .collect(Collectors.joining(", ")));

            sql.append(", primary key (");
            sql.append(gkcs.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sql.append(")");

            sql.append(");");
            System.out.println(sql.toString());
            return null;
        };

        PkThreader r = new PkThreader(
                pq.idAllocator,
                pq.planner.getSymbolAllocator(),
                pq.session,
                pq.lqr.getMetadata(),
                pq.connectorSupport,
                isp);

        PlanNode newRoot = pq.plan.getRoot().accept(r, ctx);
        System.out.println(newRoot);
        System.out.println(ctx);
    }
}