package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.Configs;
import com.wrmsr.presto.util.config.merging.StringMapMergingConfigNode;

import java.util.Map;

public final class JvmConfig
    extends StringMapMergingConfigNode<JvmConfig> implements MainConfigNode<JvmConfig>
{
    @JsonCreator
    public static JvmConfig valueOf(Object object)
    {
        return new JvmConfig(Configs.flatten(object));
    }

    public JvmConfig(Map<String, String> entries)
    {
        super(entries);
    }
}
