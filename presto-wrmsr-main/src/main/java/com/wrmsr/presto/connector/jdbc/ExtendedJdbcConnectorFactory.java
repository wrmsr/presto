/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.connector.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcConnector;
import com.facebook.presto.plugin.jdbc.JdbcHandleResolver;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcModule;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcRecordSinkProvider;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import com.wrmsr.presto.util.config.Configs;
import io.airlift.bootstrap.Bootstrap;
import org.apache.commons.configuration.HierarchicalConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.collect.Maps.mapMerge;

public class ExtendedJdbcConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Module module;
    private final Map<String, String> optionalConfig;
    private final Map<String, String> defaultConfig;
    private final ClassLoader classLoader;

    public ExtendedJdbcConnectorFactory(String name, Module module, Map<String, String> optionalConfig, Map<String, String> defaultConfig, ClassLoader classLoader)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = checkNotNull(module, "module is null");
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
        this.defaultConfig = defaultConfig;
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    public static class Config
    {
        private final Map<String, Object> extraProperties = new HashMap<>();

        @JsonAnyGetter
        public Map<String, Object> getExtraProperties()
        {
            return extraProperties;
        }

        @JsonAnySetter
        public void setExtraProperty(final String name, final Object value)
        {
            extraProperties.put(name, value);
        }
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig)
    {
        checkNotNull(requiredConfig, "requiredConfig is null");
        checkNotNull(optionalConfig, "optionalConfig is null");

        Map<String, String> workingConfig = mapMerge(defaultConfig, requiredConfig);
        HierarchicalConfiguration hc = Configs.CONFIG_PROPERTIES_CODEC.decode(workingConfig);
        Config config = Configs.OBJECT_CONFIG_CODEC.decode(hc, Config.class);

        workingConfig = Configs.CONFIG_PROPERTIES_CODEC.encode(
                Configs.OBJECT_CONFIG_CODEC.encode(config.getExtraProperties()));

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(createModules(connectorId));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(workingConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(JdbcConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    protected List<Module> createModules(String connectorId)
    {
        return ImmutableList.of(Modules.override(new JdbcModule(connectorId), module).with(new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(JdbcConnector.class).to(ExtendedJdbcConnector.class).in(Scopes.SINGLETON);
                binder.bind(JdbcRecordSetProvider.class).to(ExtendedJdbcRecordSetProvider.class).in(Scopes.SINGLETON);
                binder.bind(JdbcHandleResolver.class).to(ExtendedJdbcHandleResolver.class).in(Scopes.SINGLETON);
                binder.bind(JdbcMetadata.class).to(ExtendedJdbcMetadata.class).in(Scopes.SINGLETON);
                binder.bind(JdbcRecordSinkProvider.class).to(ExtendedJdbcRecordSinkProvider.class).in(Scopes.SINGLETON);
            }
        }));
    }

    protected static ClassLoader getClassLoader()
    {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), ExtendedJdbcConnector.class.getClassLoader());
    }
}
