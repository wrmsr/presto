package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.NodeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SplitterModule
        implements Module
{
    @Nullable
    private final NodeManager nodeManager;

    public SplitterModule(@Nullable NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    @Override
    public void configure(Binder binder)
    {
        //binder.bind(NodeManager.class)
        /*
        binder.bind(JdbcMetadata.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcConnector.class).in(Scopes.SINGLETON);
        */
        binder.bind(SplitterConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SplitterConfig.class);
    }
}
