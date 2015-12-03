package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.Configs;

import java.util.Map;

public final class LogConfigNode
    extends StringMapConfigNode
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
