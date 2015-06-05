package com.wrmsr.presto.hardcoded;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;

public class HardcodedConnector
    implements Connector
{
    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return null;
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return null;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return null;
    }
}
