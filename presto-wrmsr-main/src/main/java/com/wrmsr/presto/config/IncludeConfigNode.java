package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;

public final class IncludeConfigNode
    extends ListConfigNode<String>
{
    @JsonCreator
    public static IncludeConfigNode valueOf(Object object)
    {
        return new IncludeConfigNode(unpack(object, String.class));
    }

    public IncludeConfigNode(List<String> items)
    {
        super(items);
    }
}
