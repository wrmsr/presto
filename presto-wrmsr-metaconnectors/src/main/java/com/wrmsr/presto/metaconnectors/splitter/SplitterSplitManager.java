package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by wtimoney on 5/26/15.
 */
public class SplitterSplitManager
        implements ConnectorSplitManager
{
    private final Connector targetConnector;
    private final ConnectorSplitManager target;

    public SplitterSplitManager(Connector targetConnector, ConnectorSplitManager target)
    {
        this.targetConnector = checkNotNull(targetConnector);
        this.target = checkNotNull(target);
    }

    @Override
    @Deprecated
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, TupleDomain<ColumnHandle> tupleDomain)
    {
        return target.getPartitions(table, tupleDomain);
    }

    @Override
    @Deprecated
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        Metadata metadata = targetConnector.getMetadata()
        List<ColumnMetadata> columns = metadata.getTableMetadata(table).getColumns();
        ColumnMetadata idColumn = columns.stream().filter(c -> "id".equals(c.getName())).findFirst().get();
        ColumnHandle idColumnHandle = targetConnector.getMetadata().getColumnHandles(table).get("id");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(
                        idColumnHandle, Domain.create(SortedRangeSet.of(
                                Range.lessThan(1000), Range.greaterThanOrEqual(1000)
                ), false))
        );

        return new SplitterSplitSource(target.getPartitionSplits(table, partitions));
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
