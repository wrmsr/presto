package com.wrmsr.presto.hardcoded;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class HardcodedModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConnectorHandleResolver.class).to(HardcodedHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(HardcodedMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(HardcodedSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(HardcodedRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(HardcodedConnector.class).in(Scopes.SINGLETON);
        // configBinder(binder).bindConfig(HardcodedConfig.class);
    }
}
