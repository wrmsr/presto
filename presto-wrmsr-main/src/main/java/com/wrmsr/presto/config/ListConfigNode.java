package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public abstract class ListConfigNode<T>
    extends ConfigNode
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

    private final List<T> items;

    public ListConfigNode(List<T> items)
    {
        this.items = ImmutableList.copyOf(items);
    }

    @JsonValue
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
