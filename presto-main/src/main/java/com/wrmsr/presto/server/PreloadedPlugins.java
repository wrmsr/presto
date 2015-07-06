package com.wrmsr.presto.server;

import com.google.inject.Module;

public class PreloadedPlugins
{
    private PreloadedPlugins()
    {
    }

    public static Iterable<Module> processServerModules(Iterable<Module> modules)
    {
        return modules;
    }
}
