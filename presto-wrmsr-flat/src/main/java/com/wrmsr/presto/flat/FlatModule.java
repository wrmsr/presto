package com.wrmsr.presto.flat;

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
        binder.bind(FlatConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(FlatConfig.class);
    }
}
