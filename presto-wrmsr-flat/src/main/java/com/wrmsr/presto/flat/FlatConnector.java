package com.wrmsr.presto.flat;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class FlatConnector
    implements Connector
{
    private final ConnectorHandleResolver connectorHandleResolver;
    private final ConnectorMetadata connectorMetadata;
    private final ConnectorSplitManager connectorSplitManager;
    private final ConnectorRecordSetProvider connectorRecordSetProvider;

    @Inject
    public FlatConnector(ConnectorHandleResolver connectorHandleResolver, ConnectorMetadata connectorMetadata, ConnectorSplitManager connectorSplitManager, ConnectorRecordSetProvider connectorRecordSetProvider)
    {
        this.connectorHandleResolver = checkNotNull(connectorHandleResolver);
        this.connectorMetadata = checkNotNull(connectorMetadata);
        this.connectorSplitManager = checkNotNull(connectorSplitManager);
        this.connectorRecordSetProvider = connectorRecordSetProvider;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return connectorHandleResolver;
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return connectorMetadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return connectorSplitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return connectorRecordSetProvider;
    }
}
