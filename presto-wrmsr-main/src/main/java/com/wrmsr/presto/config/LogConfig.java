package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.Configs;
import com.wrmsr.presto.util.config.merging.StringMapMergingConfigNode;

import java.util.Map;

public final class LogConfig
    extends StringMapMergingConfigNode<LogConfig> implements MainConfigNode<LogConfig>
{
    @JsonCreator
    public static LogConfig valueOf(Object object)
    {
        return new LogConfig(Configs.flatten(object));
    }

    public LogConfig(Map<String, String> entries)
    {
        super(entries);
    }
}
