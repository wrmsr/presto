package com.wrmsr.presto.flat;

import com.facebook.presto.spi.*;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class FlatConnector
    implements Connector
{
    private final ConnectorHandleResolver connectorHandleResolver;
    private final ConnectorMetadata connectorMetadata;
    private final ConnectorSplitManager connectorSplitManager;
    private final ConnectorRecordSetProvider connectorRecordSetProvider;
    private final ConnectorRecordSinkProvider connectorRecordSinkProvider;

    @Inject
    public FlatConnector(ConnectorHandleResolver connectorHandleResolver, ConnectorMetadata connectorMetadata, ConnectorSplitManager connectorSplitManager, ConnectorRecordSetProvider connectorRecordSetProvider, ConnectorRecordSinkProvider connectorRecordSinkProvider)
    {
        this.connectorHandleResolver = checkNotNull(connectorHandleResolver);
        this.connectorMetadata = checkNotNull(connectorMetadata);
        this.connectorSplitManager = checkNotNull(connectorSplitManager);
        this.connectorRecordSetProvider = connectorRecordSetProvider;
        this.connectorRecordSinkProvider = connectorRecordSinkProvider;
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

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return connectorRecordSinkProvider;
    }
}
