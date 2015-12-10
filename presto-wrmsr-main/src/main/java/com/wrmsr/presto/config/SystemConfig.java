package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.Configs;
import com.wrmsr.presto.util.config.merging.StringMapMergingConfigNode;

import java.util.Map;

public final class SystemConfig
    extends StringMapMergingConfigNode<SystemConfig> implements MainConfigNode<SystemConfig>
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
