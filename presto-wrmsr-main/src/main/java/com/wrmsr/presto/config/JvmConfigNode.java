package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;

public class JvmConfigNode
    extends StringListConfigNode
{
    @JsonCreator
    public static JvmConfigNode valueOf(List<String> items)
    {
        return new JvmConfigNode(items);
    }

    public JvmConfigNode(List<String> items)
    {
        super(items);
    }
}
