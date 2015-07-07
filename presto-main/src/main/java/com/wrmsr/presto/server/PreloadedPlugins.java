package com.wrmsr.presto.server;

import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.Plugin;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.airlift.resolver.ArtifactResolver;

import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class PreloadedPlugins
{
    private static final Logger log = Logger.get(PreloadedPlugins.class);

    private PreloadedPlugins()
    {
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
        Module module = Modules.combine(modules);
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
        return ImmutableList.of(new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.install(finalModule);
                for (Plugin plugin : preloadedPlugins) {
                    newSetBinder(binder, Plugin.class).addBinding().toInstance(plugin);
                }
            }
        });
    }
}
