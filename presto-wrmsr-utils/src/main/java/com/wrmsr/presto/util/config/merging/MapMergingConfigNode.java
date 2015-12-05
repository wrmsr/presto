package com.wrmsr.presto.util.config.merging;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public abstract class MapMergingConfigNode<K, V>
    implements MergingConfigNode
{
    protected final Map<K, V> entries;

    public MapMergingConfigNode(Map<K, V> entries)
    {
        this.entries = ImmutableMap.copyOf(entries);
    }

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
