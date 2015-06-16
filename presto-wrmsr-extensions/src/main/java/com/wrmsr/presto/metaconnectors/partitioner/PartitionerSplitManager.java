package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.wrmsr.presto.util.ImmutableCollectors;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;

public class PartitionerSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final Connector targetConnector;
    private final ConnectorSplitManager target;
    private final Partitioner partitioner;

    public PartitionerSplitManager(String connectorId, Connector targetConnector, ConnectorSplitManager target, Partitioner partitioner)
    {
        this.connectorId = connectorId;
        this.targetConnector = checkNotNull(targetConnector);
        this.target = checkNotNull(target);
        this.partitioner = checkNotNull(partitioner);
    }

    @Override
    @Deprecated
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, TupleDomain<ColumnHandle> tupleDomain)
    {
        ConnectorMetadata metadata = targetConnector.getMetadata();
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(table);
        TupleDomain<String> stringTupleDomain = TupleDomain.all();
        for (Map.Entry<ColumnHandle, Domain> e : tupleDomain.getDomains().entrySet()) {
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(table, e.getKey());
            stringTupleDomain = TupleDomain.columnWiseUnion(
                    stringTupleDomain,
                    TupleDomain.withColumnDomains(ImmutableMap.of(columnMetadata.getName(), e.getValue())));
        }

        List<ColumnMetadata> columns = metadata.getTableMetadata(table).getColumns();
        // ColumnMetadata idColumn = columns.stream().filter(c -> "id".equals(c.getName())).findFirst().get();
        ColumnHandle idColumnHandle = targetConnector.getMetadata().getColumnHandles(table).get("id");
        /*
        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        idColumnHandle, Domain.create(SortedRangeSet.of(
                                Range.lessThan(1000L), Range.greaterThanOrEqual(1000L)
                        ), false))
        );
        */

        List<Domain> idDomains = ImmutableList.of(
                Domain.create(SortedRangeSet.of(
                        // Range.equal(1000L)
                        Range.range(0L, true, 1000L, false)
                ), false),
                Domain.create(SortedRangeSet.of(
                        // Range.equal(2000L)
                        Range.range(1000L, true, 2000L, false)
                ), false),
                Domain.create(SortedRangeSet.of(
                        // Range.equal(2000L)
                        Range.range(2000L, true, 3000L, false)
                ), false)
        );

        List<ConnectorPartition> partitions = newArrayList();
        TupleDomain<ColumnHandle> undeterminedTupleDomain = TupleDomain.none();
        for (Domain idDomain : idDomains) {
            TupleDomain intersectedDomain = tupleDomain.intersect(
                TupleDomain.withColumnDomains(ImmutableMap.of(idColumnHandle, idDomain)));
            if (intersectedDomain.isNone()) {
                continue;
            }

            ConnectorPartitionResult r = target.getPartitions(table, intersectedDomain);
            partitions.addAll(r.getPartitions());

            // if (r.getUndeterminedTupleDomain()) // FIXME
            undeterminedTupleDomain = TupleDomain.columnWiseUnion(
                    undeterminedTupleDomain,
                    r.getUndeterminedTupleDomain().intersect(intersectedDomain));
        }

        return new ConnectorPartitionResult(Collections.unmodifiableList(partitions), undeterminedTupleDomain);
        //return new PartitionerSplitSource(target.getPartitionSplits(table, partitions));
    }

    @Override
    @Deprecated
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        List<ConnectorSplit> splits = partitions.stream()
                .map(p -> target.getPartitionSplits(table, ImmutableList.of(p)))
                .flatMap(s -> {
                    try
                    {
                        return s.getNextBatch(Integer.MAX_VALUE).get().stream();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(ImmutableCollectors.toImmutableList());
        return new FixedSplitSource(connectorId, splits);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTableLayoutHandle layout)
    {
        return target.getSplits(layout);
    }

    /*
    @Override
    public ConnectorSplitSource getSplits(ConnectorTableLayoutHandle layout)
    {
        TpchTableHandle tableHandle = checkType(layout, TpchTableLayoutHandle.class, "layout").getTable();

        Set<Node> nodes = nodeManager.getActiveDatasourceNodes(connectorId);
        checkState(!nodes.isEmpty(), "No TPCH nodes available");

        int totalParts = nodes.size() * splitsPerNode;
        int partNumber = 0;

        // Split the data using split and skew by the number of nodes available.
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (Node node : nodes) {
            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(new TpchSplit(tableHandle, partNumber, totalParts, ImmutableList.of(node.getHostAndPort())));
                partNumber++;
            }
        }
        return new FixedSplitSource(connectorId, splits.build());
    }
    */
}
