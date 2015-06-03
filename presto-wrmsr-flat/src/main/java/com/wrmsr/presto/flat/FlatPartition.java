package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.TupleDomain;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class FlatPartition
    implements ConnectorPartition
{
    private final FlatTableHandle flatTableHandle;
    private final TupleDomain<ColumnHandle> domain;

    public FlatPartition(FlatTableHandle flatTableHandle, TupleDomain<ColumnHandle> domain)
    {
        this.flatTableHandle = checkNotNull(flatTableHandle, "flatTableHandle is null");
        this.domain = checkNotNull(domain, "domain is null");
    }

    @Override
    public String getPartitionId()
    {
        return flatTableHandle.toString();
    }

    public FlatTableHandle getFlatTableHandle()
    {
        return flatTableHandle;
    }

    @Override
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return domain;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("flatTableHandle", flatTableHandle)
                .toString();
    }
}
