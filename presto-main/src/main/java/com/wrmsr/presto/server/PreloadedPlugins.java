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
package com.wrmsr.presto.server;

import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.spi.Plugin;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import io.airlift.resolver.ArtifactResolver;

import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class PreloadedPlugins
{
    private static final Logger log = Logger.get(PreloadedPlugins.class);

    private PreloadedPlugins()
    {
    }

    public static class CombineConfigurationAwareModule extends AbstractConfigurationAwareModule
    {
        private final Set<Module> modulesSet;
        private ConfigurationFactory configurationFactory; // wow fuck you.

        public CombineConfigurationAwareModule(Iterable<? extends Module> modules)
        {
            this.modulesSet = ImmutableSet.copyOf(modules);
        }

        @Override
        public synchronized void setConfigurationFactory(ConfigurationFactory configurationFactory)
        {
            super.setConfigurationFactory(configurationFactory);
            this.configurationFactory = configurationFactory;
        }

        @Override
        protected void setup(Binder binder)
        {
            binder = binder.skipSources(getClass());
            for (Module module : modulesSet) {
                if (module instanceof ConfigurationAwareModule) {
                    ((ConfigurationAwareModule) module).setConfigurationFactory(configurationFactory);
                }
                binder.install(module);
            }
        }
    }

    public static Iterable<Module> processServerModules(Iterable<Module> modules)
    {
        Bootstrap app = new Bootstrap(ImmutableList.of(new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                configBinder(binder).bindConfig(PluginManagerConfig.class);
            }
        }));
        PluginManagerConfig config;
        try {

            Injector injector = app.initialize();
            config = injector.getInstance(PluginManagerConfig.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        final Set<Plugin> preloadedPlugins = newHashSet();
        Module module = new CombineConfigurationAwareModule(modules);
        ArtifactResolver resolver = new ArtifactResolver(config.getMavenLocalRepository(), config.getMavenRemoteRepository());

        for (String preloadedPluginStr : config.getPreloadedPlugins()) {
            ClassLoader pluginClassLoader;
            try {
                pluginClassLoader = PluginManager.buildClassLoader(resolver, preloadedPluginStr);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
            ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
            List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);
            checkState(plugins.size() == 1);
            Plugin plugin = plugins.get(0);

            for (ModuleProcessor moduleProcessor : plugin.getServices(ModuleProcessor.class)) {
                log.info("Handling module processor: ", moduleProcessor);
                module = moduleProcessor.apply(module);
            }

            preloadedPlugins.add(plugin);
        }

        final Module finalModule = module;
        return ImmutableList.of(
                new CombineConfigurationAwareModule(ImmutableSet.of(
                        finalModule,
                        new Module()
                        {
                            @Override
                            public void configure(Binder binder)
                            {
                                for (Plugin plugin : preloadedPlugins) {
                                    newSetBinder(binder, Plugin.class).addBinding().toInstance(plugin);
                                }
                            }
                        })));
    }
}
