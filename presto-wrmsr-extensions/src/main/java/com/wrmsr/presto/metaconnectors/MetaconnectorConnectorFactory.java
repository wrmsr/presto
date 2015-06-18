package com.wrmsr.presto.metaconnectors;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.wrmsr.presto.metaconnectors.partitioner.Partitioner;
import com.wrmsr.presto.metaconnectors.partitioner.PartitionerConnector;
import com.wrmsr.presto.metaconnectors.partitioner.PartitionerConnectorId;
import com.wrmsr.presto.metaconnectors.partitioner.PartitionerTarget;
import com.wrmsr.presto.util.Configs;
import io.airlift.bootstrap.Bootstrap;

import java.util.Arrays;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;

public abstract class MetaconnectorConnectorFactory implements ConnectorFactory
{
    private final Map<String, String> optionalConfig;
    private final Module module;
    private final ClassLoader classLoader;
    private final ConnectorManager connectorManager;

    public MetaconnectorConnectorFactory(Map<String, String> optionalConfig, Module module, ClassLoader classLoader, ConnectorManager connectorManager)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
        this.module = checkNotNull(module, "module is null");
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
        this.connectorManager = checkNotNull(connectorManager, "connectorManager is null");
    }

    public Map<String, String> getOptionalConfig()
    {
        return optionalConfig;
    }

    public Module getModule()
    {
        return module;
    }

    public ClassLoader getClassLoader()
    {
        return classLoader;
    }

    public ConnectorManager getConnectorManager()
    {
        return connectorManager;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> properties)
    {
        checkNotNull(properties, "properties is null");
        String targetName = checkNotNull(properties.get("target-name")); // FIXME: default %s_
        String targetConnectorName = properties.get("target-connector-name");

        Connector target;
        Map<String, String> requiredConfiguration;

        if (targetConnectorName == null) {
            target = checkNotNull(connectorManager.getConnectors().get(targetName), "target-connector-name not specified and target not found");
            requiredConfiguration = ImmutableMap.of();

        } else {
            requiredConfiguration = newHashMap(properties);
            Map<String, String> targetProperties = Configs.stripSubconfig(requiredConfiguration, "target");

            connectorManager.createConnection(targetName, targetConnectorName, targetProperties);
            target = checkNotNull(connectorManager.getConnectors().get(targetName));
        }

        return create(target, connectorId, requiredConfiguration);
    }

    public abstract Connector create(Connector target, String connectorId, Map<String, String> requiredConfiguration);

    protected Connector createWithOverride(Map<String, String> requiredConfiguration, Module... modules)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(ImmutableList.<Module>builder().add(module).addAll(Arrays.asList(modules)).build());

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfiguration)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(PartitionerConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
