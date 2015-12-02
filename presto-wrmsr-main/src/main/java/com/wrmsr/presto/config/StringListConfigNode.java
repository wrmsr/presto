package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;

import java.util.List;

public abstract class StringListConfigNode
    extends ConfigNode
{
    private final List<String> items;

    public StringListConfigNode(List<String> items)
    {
        this.items = ImmutableList.copyOf(items);
    }

    @JsonValue
    public List<String> getItems()
    {
        return items;
    }

    @Override
    public String toString()
    {
        return getClass().getName() + "{" +
                "items=" + items +
                '}';
    }
}
