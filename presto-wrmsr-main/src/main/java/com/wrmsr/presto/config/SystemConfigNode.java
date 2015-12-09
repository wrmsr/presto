package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.Configs;
import com.wrmsr.presto.util.config.merging.StringMapMergingConfigNode;

import java.util.Map;

public final class SystemConfigNode
    extends StringMapMergingConfigNode<SystemConfigNode> implements MainConfigNode<SystemConfigNode>
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
