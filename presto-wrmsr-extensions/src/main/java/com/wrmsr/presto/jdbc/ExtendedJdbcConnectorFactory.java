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
package com.wrmsr.presto.jdbc;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import com.wrmsr.presto.util.Configs;
import io.airlift.bootstrap.Bootstrap;
import org.apache.commons.configuration.HierarchicalConfiguration;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.wrmsr.presto.util.Maps.mapMerge;

public class ExtendedJdbcConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Module module;
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;

    public ExtendedJdbcConnectorFactory(String name, Module module, Map<String, String> optionalConfig, ClassLoader classLoader)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = checkNotNull(module, "module is null");
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig)
    {
        checkNotNull(requiredConfig, "requiredConfig is null");
        checkNotNull(optionalConfig, "optionalConfig is null");

        HierarchicalConfiguration config = Configs.toHierarchical(mapMerge(optionalConfig, requiredConfig));
        List<String> initScripts = Configs.getAllStrings(config, "init");
        requiredConfig = Configs.stripSubconfig(requiredConfig, "init");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(createModules(connectorId));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            injector.getInstance(ExtendedJdbcConfig.class).getInitScripts().addAll(initScripts);

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
                binder.bind(JdbcRecordSetProvider.class).to(ExtendedJdbcRecordSetProvider.class).in(Scopes.SINGLETON);
                binder.bind(JdbcHandleResolver.class).to(ExtendedJdbcHandleResolver.class).in(Scopes.SINGLETON);
            }
        }));
    }
}
