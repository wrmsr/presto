package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.wrmsr.presto.util.Configs;
import com.wrmsr.presto.util.ImmutableCollectors;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.MapConfiguration;

import java.util.Map;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;

public class PartitionerConnectorFactory implements ConnectorFactory
{
    private final Map<String, String> optionalConfig;
    private final Module module;
    private final ClassLoader classLoader;
    private final ConnectorManager connectorManager;

    public PartitionerConnectorFactory(Map<String, String> optionalConfig, Module module, ClassLoader classLoader, ConnectorManager connectorManager)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
        this.module = checkNotNull(module, "module is null");
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
        this.connectorManager = checkNotNull(connectorManager, "connectorManager is null");
    }

    @Override
    public String getName()
    {
        return "partitioner";
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> properties)
    {
        checkNotNull(properties, "properties is null");
        String targetName = checkNotNull(properties.get("target-name"));
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

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(module, new Module()
            {
                @Override
                public void configure(Binder binder)
                {
                    binder.bind(PartitionerConnectorId.class).toInstance(new PartitionerConnectorId(connectorId));
                    binder.bind(PartitionerTarget.class).toInstance(new PartitionerTarget(target));
                }
            });

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
