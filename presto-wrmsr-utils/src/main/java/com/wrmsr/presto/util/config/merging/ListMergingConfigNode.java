package com.wrmsr.presto.util.config.merging;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public abstract class ListMergingConfigNode<N extends ListMergingConfigNode<N, T>, T>
    implements MergingConfigNode<N>, Iterable<T>
{
    @SuppressWarnings({"unchecked"})
    public static <T> List<T> unpack(Object object, Class<T> cls)
    {
        if (object instanceof List) {
            return (List<T>) object;
        }
        else if (cls.isInstance(object)) {
            return ImmutableList.of((T) object);
        }
        else {
            throw new IllegalArgumentException(Objects.toString(object));
        }
    }

    protected final List<T> items;

    public ListMergingConfigNode(List<T> items)
    {
        this.items = ImmutableList.copyOf(items);
    }

    public List<T> getItems()
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

    @SuppressWarnings({"unchecked"})
    @Override
    public N merge(N other)
    {
        List mergedList = ImmutableList.builder()
                .addAll(items)
                .addAll(other.getItems())
                .build();
        N merged;
        try {
            merged = (N) getClass().getConstructor(List.class).newInstance(mergedList);
        }
        catch (IllegalAccessException | NoSuchMethodException | InstantiationException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
        return merged;
    }

    public static <N extends ListMergingConfigNode<N, T>, T> ListMergingConfigNode<N, T> newDefault(Class<N> cls)
    {
        try {
            return cls.getConstructor(List.class).newInstance(ImmutableList.of());
        }
        catch (IllegalAccessException | NoSuchMethodException | InstantiationException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        return items.iterator();
    }
}
