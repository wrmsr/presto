package com.wrmsr.presto.flat;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class FlatConnectorFactory
    implements ConnectorFactory
{
    private final Map<String, String> optionalConfig;
    private final Module module;
    private final ClassLoader classLoader;

    public FlatConnectorFactory(Map<String, String> optionalConfig, Module module, ClassLoader classLoader)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
        this.module = checkNotNull(module, "module is null");
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "flat";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfiguration)
    {
        checkNotNull(requiredConfiguration, "requiredConfiguration is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(module, new Module()
            {
                @Override
                public void configure(Binder binder)
                {
                    binder.bind(FlatConnectorId.class).toInstance(new FlatConnectorId(connectorId));
                }
            });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfiguration)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(FlatConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
