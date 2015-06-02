package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.SystemTable;
import com.google.inject.Inject;

import java.util.Set;

public class PartitionerConnector
    implements Connector
{
    private final PartitionerConnectorId connectorId;
    private final PartitionerTarget target;

    @Inject
    public PartitionerConnector(PartitionerConnectorId connectorId, PartitionerTarget target)
    {
        this.connectorId = connectorId;
        this.target = target;
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return new PartitionerMetadata(target.getTarget().getMetadata());
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new PartitionerSplitManager(connectorId.toString(), target.getTarget(), target.getTarget().getSplitManager());
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return target.getTarget().getHandleResolver();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return target.getTarget().getPageSourceProvider();
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return target.getTarget().getRecordSetProvider();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return target.getTarget().getPageSinkProvider();
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return target.getTarget().getRecordSinkProvider();
    }

    @Override
    public ConnectorIndexResolver getIndexResolver()
    {
        return target.getTarget().getIndexResolver();
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return target.getTarget().getSystemTables();
    }

    @Override
    public void shutdown()
    {
        target.getTarget().shutdown();
    }
}
