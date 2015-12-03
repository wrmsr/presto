package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.Configs;

import java.util.Map;

public final class JvmConfigNode
    extends StringMapConfigNode
{
    @JsonCreator
    public static JvmConfigNode valueOf(Object object)
    {
        return new JvmConfigNode(Configs.flatten(object));
    }

    public JvmConfigNode(Map<String, String> entries)
    {
        super(entries);
    }
}
