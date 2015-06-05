package com.wrmsr.presto.hardcoded;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.google.inject.Inject;

public class HardcodedConnector
    implements Connector
{
    private final ConnectorHandleResolver connectorHandleResolver;
    private final ConnectorMetadata connectorMetadata;
    private final ConnectorSplitManager connectorSplitManager;
    private final ConnectorRecordSetProvider connectorRecordSetProvider;

    private final HardcodedContents hardcodedContents;

    @Inject
    public HardcodedConnector(ConnectorMetadata connectorMetadata, ConnectorSplitManager connectorSplitManager, ConnectorRecordSetProvider connectorRecordSetProvider, ConnectorHandleResolver connectorHandleResolver, HardcodedContents hardcodedContents)
    {
        this.connectorHandleResolver = connectorHandleResolver;
        this.connectorMetadata = connectorMetadata;
        this.connectorSplitManager = connectorSplitManager;
        this.connectorRecordSetProvider = connectorRecordSetProvider;
        this.hardcodedContents = hardcodedContents;
    }

    public HardcodedContents getHardcodedContents()
    {
        return hardcodedContents;
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
