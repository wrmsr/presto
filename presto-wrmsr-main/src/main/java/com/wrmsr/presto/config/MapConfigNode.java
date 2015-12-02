package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public abstract class MapConfigNode<K, V>
    extends ConfigNode
{
    private final Map<K, V> entries;

    public MapConfigNode(Map<K, V> entries)
    {
        this.entries = ImmutableMap.copyOf(entries);
    }

    @JsonValue
    public Map<K, V> getEntries()
    {
        return entries;
    }

    @Override
    public String toString()
    {
        return getClass().getName() + "{" +
                "entries=" + entries +
                '}';
    }
}
