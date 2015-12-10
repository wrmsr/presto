package com.wrmsr.presto.config;

import com.google.inject.Binder;
import com.google.inject.Module;

public class ConfigModule
        implements Module
{
    private final MainConfig config;

    public ConfigModule(MainConfig config)
    {
        this.config = config;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConnectorsConfig.class).toInstance(config.getMergedNode(ConnectorsConfig.class));
        binder.bind(ExecConfig.class).toInstance(config.getMergedNode(ExecConfig.class));
        binder.bind(JvmConfig.class).toInstance(config.getMergedNode(JvmConfig.class));
        binder.bind(LogConfig.class).toInstance(config.getMergedNode(LogConfig.class));
        binder.bind(PluginsConfig.class).toInstance(config.getMergedNode(PluginsConfig.class));
        binder.bind(ScriptingConfig.class).toInstance(config.getMergedNode(ScriptingConfig.class));
        binder.bind(SystemConfig.class).toInstance(config.getMergedNode(SystemConfig.class));
    }
}
