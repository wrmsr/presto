package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;

public final class PluginsConfigNode
    extends StringListConfigNode
{
    @JsonCreator
    public static PluginsConfigNode valueOf(List<String> items)
    {
        return new PluginsConfigNode(items);
    }

    public PluginsConfigNode(List<String> items)
    {
        super(items);
    }
}
