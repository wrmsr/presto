package com.wrmsr.presto.flat;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;

public class FlatConnector
    implements Connector
{
    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new FlatHandleResolver();
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return new FlatMetadata();
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new FlatSplitManager();
    }
}
