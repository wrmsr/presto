package com.wrmsr.presto.hardcoded;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.wrmsr.presto.util.Configs;
import io.airlift.bootstrap.Bootstrap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkNotNull;

public class HardcodedConnectorFactory
    implements ConnectorFactory
{
    private final Map<String, String> optionalConfig;
    private final Module module;
    private final ClassLoader classLoader;

    public HardcodedConnectorFactory(Map<String, String> optionalConfig, Module module, ClassLoader classLoader)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
        this.module = checkNotNull(module, "module is null");
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "hardcoded";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfiguration)
    {
        checkNotNull(requiredConfiguration, "requiredConfiguration is null");
        requiredConfiguration = Maps.newHashMap(requiredConfiguration);

        Map<String, String> views = Configs.stripSubconfig(requiredConfiguration, "views");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(module, new Module()
            {
                @Override
                public void configure(Binder binder)
                {
                    binder.bind(HardcodedConnectorId.class).toInstance(new HardcodedConnectorId(connectorId));
                }
            });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfiguration)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(HardcodedConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
