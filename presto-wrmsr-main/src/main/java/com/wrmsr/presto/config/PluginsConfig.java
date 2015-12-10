package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.merging.StringListMergingConfigNode;

import java.util.List;

public final class PluginsConfig
    extends StringListMergingConfigNode<PluginsConfig> implements MainConfigNode<PluginsConfig>
{
    @JsonCreator
    public PluginsConfig(List<String> items)
    {
        super(items);
    }
}
