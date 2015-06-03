package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;

import javax.inject.Inject;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class FlatHandleResolver
    implements ConnectorHandleResolver
{
    private final String connectorId;

    @Inject
    public FlatHandleResolver(FlatConnectorId connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return tableHandle instanceof FlatTableHandle && ((FlatTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof FlatColumnHandle && ((FlatColumnHandle) columnHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return split instanceof FlatSplit && ((FlatSplit) split).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ConnectorOutputTableHandle tableHandle)
    {
        return (tableHandle instanceof FlatOutputTableHandle) && ((FlatOutputTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return FlatTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return FlatColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return FlatSplit.class;
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        return FlatOutputTableHandle.class;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .toString();
    }
}
