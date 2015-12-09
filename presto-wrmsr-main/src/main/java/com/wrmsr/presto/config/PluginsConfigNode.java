package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.merging.StringListMergingConfigNode;

import java.util.List;

public final class PluginsConfigNode
    extends StringListMergingConfigNode<PluginsConfigNode> implements MainConfigNode<PluginsConfigNode>
{
    @JsonCreator
    public PluginsConfigNode(List<String> items)
    {
        super(items);
    }
}
