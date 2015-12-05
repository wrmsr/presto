package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.Configs;
import com.wrmsr.presto.util.config.merging.StringMapMergingConfigNode;

import java.util.Map;

public final class LogConfigNode
    extends StringMapMergingConfigNode
        implements MainConfigNode
{
    @JsonCreator
    public static LogConfigNode valueOf(Object object)
    {
        return new LogConfigNode(Configs.flatten(object));
    }

    public LogConfigNode(Map<String, String> entries)
    {
        super(entries);
    }
}
