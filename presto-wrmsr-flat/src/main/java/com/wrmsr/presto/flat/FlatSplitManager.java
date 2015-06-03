package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.TupleDomain;

public class FlatSplitManager
    implements ConnectorSplitManager
{
    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, TupleDomain<ColumnHandle> tupleDomain)
    {
        return null;
    }
}
