package com.wrmsr.presto.util.config.merging;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public abstract class UnknownMergingConfigNode<N extends UnknownMergingConfigNode<N>>
    implements MergingConfigNode<N>
{
    private final String type;
    private final Object object;

    public UnknownMergingConfigNode(String type, Object object)
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
}
