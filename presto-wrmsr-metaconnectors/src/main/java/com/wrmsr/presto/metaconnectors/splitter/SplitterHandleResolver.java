package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by wtimoney on 5/26/15.
 */
public class SplitterHandleResolver
        implements ConnectorHandleResolver
{
    private final String connectorId;

    public SplitterHandleResolver(String connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return false;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return false;
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return false;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return null;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return null;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return null;
    }
}
