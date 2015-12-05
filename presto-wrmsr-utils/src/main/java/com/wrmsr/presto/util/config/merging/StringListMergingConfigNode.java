package com.wrmsr.presto.util.config.merging;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.List;

public abstract class StringListMergingConfigNode
    extends ListMergingConfigNode<String>
{
    public StringListMergingConfigNode(List<String> items)
    {
        super(items);
    }

    @JsonValue
    public List<String> getItems()
    {
        return items;
    }
}
