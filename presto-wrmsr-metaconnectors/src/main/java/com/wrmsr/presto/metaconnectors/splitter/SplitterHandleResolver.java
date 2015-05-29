package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;

import static com.google.common.base.Preconditions.checkNotNull;

public class SplitterHandleResolver
        implements ConnectorHandleResolver
{
    private final String connectorId;
    private final ConnectorHandleResolver target;

    public SplitterHandleResolver(String connectorId, ConnectorHandleResolver target)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.target = checkNotNull(target, "target is null");
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return target.canHandle(tableHandle);
    }

    @Override
    public boolean canHandle(ConnectorTableLayoutHandle handle)
    {
        return target.canHandle(handle);
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return target.canHandle(columnHandle);
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return split instanceof SplitterSplit && target.canHandle(((SplitterSplit) split).getTarget());
    }

    @Override
    public boolean canHandle(ConnectorIndexHandle indexHandle)
    {
        return target.canHandle(indexHandle);
    }

    @Override
    public boolean canHandle(ConnectorOutputTableHandle tableHandle)
    {
        return target.canHandle(tableHandle);
    }

    @Override
    public boolean canHandle(ConnectorInsertTableHandle tableHandle)
    {
        return target.canHandle(tableHandle);
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return target.getTableHandleClass();
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        return target.getTableLayoutHandleClass();
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return target.getColumnHandleClass();
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return target.getSplitClass();
    }

    @Override
    public Class<? extends ConnectorIndexHandle> getIndexHandleClass()
    {
        return target.getIndexHandleClass();
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        return target.getOutputTableHandleClass();
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        return target.getInsertTableHandleClass();
    }
}
