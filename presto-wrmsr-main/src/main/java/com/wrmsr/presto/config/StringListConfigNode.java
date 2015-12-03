package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.List;

public abstract class StringListConfigNode
    extends ListConfigNode<String>
{
    public StringListConfigNode(List<String> items)
    {
        super(items);
    }

    @JsonValue
    public List<String> getItems()
    {
        return items;
    }
}
