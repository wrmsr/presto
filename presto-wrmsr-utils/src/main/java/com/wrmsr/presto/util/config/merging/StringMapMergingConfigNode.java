package com.wrmsr.presto.util.config.merging;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;

public abstract class StringMapMergingConfigNode
    extends MapMergingConfigNode<String, String>
{
    public StringMapMergingConfigNode(Map<String, String> entries)
    {
        super(entries);
    }

    @JsonValue
    public Map<String, String> getEntries()
    {
        return entries;
    }
}
