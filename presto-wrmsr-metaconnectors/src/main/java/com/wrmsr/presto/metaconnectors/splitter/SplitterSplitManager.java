package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.TupleDomain;

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
    private final String connectorId;
    private final ConnectorSplitManager target;
    private final Connector targetConnector;
    private final NodeManager nodeManager;
    private final int splitsPerNode;

    public SplitterSplitManager(String connectorId, ConnectorSplitManager target, Connector targetConnector, NodeManager nodeManager, int splitsPerNode)
    {
        this.connectorId = connectorId;
        this.target = target;
        this.targetConnector = checkNotNull(targetConnector, "target is null");
        this.nodeManager = nodeManager;
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");
        this.splitsPerNode = splitsPerNode;
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
        return new SplitterSplitSource(target.getPartitionSplits(table, partitions));
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTableLayoutHandle layout)
    {
        return new SplitterSplitSource(target.getSplits(layout));
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
