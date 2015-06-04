package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class FlatSplitManager
    implements ConnectorSplitManager
{
    private final String connectorId;

    @Inject
    public FlatSplitManager(FlatConnectorId connectorId)
    {
        this.connectorId = checkNotNull(connectorId).toString();
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, TupleDomain<ColumnHandle> tupleDomain)
    {
        checkArgument(table instanceof FlatTableHandle);

        return new ConnectorPartitionResult(
                ImmutableList.<ConnectorPartition>of(new FlatPartition((FlatTableHandle) table, tupleDomain)),
                tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        checkArgument(table instanceof FlatTableHandle);

        return new FixedSplitSource(connectorId, ImmutableList.of(new FlatSplit(connectorId)));
    }
}
