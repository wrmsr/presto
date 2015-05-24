package com.wrmsr.presto.common;

import com.facebook.presto.spi.*;

import java.util.Set;

public class ConnectorWrapper implements Connector
{
    private final Connector wrapped;

    public ConnectorWrapper(Connector wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return wrapped.getHandleResolver();
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return wrapped.getMetadata();
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return wrapped.getSplitManager();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return wrapped.getPageSourceProvider();
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return wrapped.getRecordSetProvider();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return wrapped.getPageSinkProvider();
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider() {
        return wrapped.getRecordSinkProvider();
    }

    @Override
    public ConnectorIndexResolver getIndexResolver() {
        return wrapped.getIndexResolver();
    }

    @Override
    public Set<SystemTable> getSystemTables() {
        return wrapped.getSystemTables();
    }

    @Override
    public void shutdown() {
        wrapped.shutdown();
    }
}
