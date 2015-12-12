package com.wrmsr.presto.util.config.merging;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class MapMergingConfigNode<N extends MapMergingConfigNode<N, K, V>, K, V>
    implements MergingConfigNode<N>, Iterable<Map.Entry<K, V>>
{
    protected final Map<K, V> entries;

    public MapMergingConfigNode()
    {
        this.entries = ImmutableMap.of();
    }

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

    @SuppressWarnings({"unchecked"})
    @Override
    public N merge(N other)
    {
        Map mergedMap = ImmutableMap.builder()
                .putAll(entries)
                .putAll(other.getEntries())
                .build();
        N merged;
        try {
            merged = (N) getClass().getConstructor(Map.class).newInstance(mergedMap);
        }
        catch (IllegalAccessException | NoSuchMethodException | InstantiationException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
        return merged;
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator()
    {
        return entries.entrySet().iterator();
    }
}
