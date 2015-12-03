package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;

public abstract class StringMapConfigNode
    extends MapConfigNode<String, String>
{
    public StringMapConfigNode(Map<String, String> entries)
    {
        super(entries);
    }

    @JsonValue
    public Map<String, String> getEntries()
    {
        return entries;
    }
}
