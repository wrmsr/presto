package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.Configs;
import com.wrmsr.presto.util.config.mergeable.StringMapMergeableConfig;

import java.util.Map;

public final class SystemConfig
        extends StringMapMergeableConfig<SystemConfig>
        implements Config<SystemConfig>
{
    @JsonCreator
    public static SystemConfig valueOf(Object object)
    {
        return new SystemConfig(Configs.flatten(object));
    }

    public SystemConfig(Map<String, String> entries)
    {
        super(entries);
    }
}
