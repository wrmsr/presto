package com.wrmsr.presto.util.config.mergeable;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.List;

public abstract class StringListMergeableConfigNode<N extends StringListMergeableConfigNode<N>>
    extends ListMergeableConfigNode<N, String>
{
    public StringListMergeableConfigNode()
    {
        super();
    }

    public StringListMergeableConfigNode(List<String> items)
    {
        super(items);
    }

    @JsonValue
    public List<String> getItems()
    {
        return items;
    }
}
