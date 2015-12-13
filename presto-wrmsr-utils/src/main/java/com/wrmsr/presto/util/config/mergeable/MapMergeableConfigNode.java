package com.wrmsr.presto.util.config.mergeable;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.Mergeable;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;

public abstract class MapMergeableConfigNode<N extends MapMergeableConfigNode<N, K, V>, K, V>
    implements MergeableConfigNode<N>, Iterable<Map.Entry<K, V>>
{
    protected final Map<K, V> entries;

    public MapMergeableConfigNode()
    {
        this.entries = ImmutableMap.of();
    }

    public MapMergeableConfigNode(Map<K, V> entries)
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
    public Mergeable merge(Mergeable other)
    {
        Map mergedMap = ImmutableMap.builder()
                .putAll(entries)
                .putAll(((N) other).getEntries())
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
