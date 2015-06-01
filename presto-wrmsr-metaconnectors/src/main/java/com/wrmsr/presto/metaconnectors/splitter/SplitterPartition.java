package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.TupleDomain;

public class SplitterPartition
    implements ConnectorPartition {

    @Override
    public String getPartitionId() {
        return null;
    }

    @Override
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return null;
    }
}
