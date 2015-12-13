package com.wrmsr.presto.util.config.mergeable;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.Mergeable;

import java.util.Map;

public abstract class UnknownMergeableConfigNode<N extends UnknownMergeableConfigNode<N>>
        implements MergeableConfigNode<N>
{
    private final String type;
    private final Object object;

    public UnknownMergeableConfigNode()
    {
        throw new UnsupportedOperationException();
    }

    public UnknownMergeableConfigNode(String type, Object object)
    {
        this.type = type;
        this.object = object;
    }

    public String getType()
    {
        return type;
    }

    public Object getObject()
    {
        return object;
    }

    @JsonValue
    public Map<String, Object> jsonValue()
    {
        return ImmutableMap.of(type, object);
    }

    @Override
    public Mergeable merge(Mergeable other)
    {
        throw new UnsupportedOperationException();
    }
}
