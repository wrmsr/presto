package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.NodeManager;
import com.google.inject.Inject;

import javax.annotation.Nullable;

public class SplitterConnector
    implements Connector
{
    private final SplitterTarget target;

    @Inject
    public SplitterConnector(SplitterTarget target)
    {
        this.target = target;
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return target.getTarget().getMetadata();
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new SplitterSplitManager(target.getTarget(), target.getTarget().getSplitManager());
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new SplitterHandleResolver(target.getTarget().getHandleResolver());
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return new SplitterRecordSetProvider(target.getTarget().getRecordSetProvider());
    }

    /*
    private int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return Integer.parseInt(firstNonNull(properties.get("splitter.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property splitter.splits-per-node");
        }
    }
    */
}
