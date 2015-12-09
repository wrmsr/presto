package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.Configs;
import com.wrmsr.presto.util.config.merging.StringMapMergingConfigNode;

import java.util.Map;

public final class JvmConfigNode
    extends StringMapMergingConfigNode<JvmConfigNode> implements MainConfigNode<JvmConfigNode>
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
