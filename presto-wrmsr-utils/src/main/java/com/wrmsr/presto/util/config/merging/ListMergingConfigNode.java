package com.wrmsr.presto.util.config.merging;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public abstract class ListMergingConfigNode<T>
    implements MergingConfigNode
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
}
