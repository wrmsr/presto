package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.Configs;

import java.util.Map;

public final class SystemConfigNode
    extends StringMapConfigNode
{
    @JsonCreator
    public static SystemConfigNode valueOf(Object object)
    {
        return new SystemConfigNode(Configs.flatten(object));
    }

    public SystemConfigNode(Map<String, String> entries)
    {
        super(entries);
    }
}
