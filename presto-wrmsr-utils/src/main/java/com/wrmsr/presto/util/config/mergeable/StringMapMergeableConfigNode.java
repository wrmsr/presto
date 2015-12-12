package com.wrmsr.presto.util.config.mergeable;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;

public abstract class StringMapMergeableConfigNode<N extends StringMapMergeableConfigNode<N>>
    extends MapMergeableConfigNode<N, String, String>
{
    public StringMapMergeableConfigNode()
    {
        super();
    }

    public StringMapMergeableConfigNode(Map<String, String> entries)
    {
        super(entries);
    }

    @JsonValue
    public Map<String, String> getEntries()
    {
        return entries;
    }
}
