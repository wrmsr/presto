package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;

public class FlatHandleResolver
    implements ConnectorHandleResolver
{
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
