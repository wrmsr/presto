package com.wrmsr.presto.flat;

import com.facebook.presto.spi.*;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class FlatModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConnectorHandleResolver.class).to(FlatHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(FlatMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(FlatSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(FlatRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSinkProvider.class).to(FlatRecordSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(FlatConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(FlatConfig.class);
    }
}

