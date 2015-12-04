package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

public final class UnknownConfigNode
    extends ConfigNode
{
    private final String type;
    private final Object object;

    public UnknownConfigNode(String type, Object object)
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
    public Object jsonValue()
    {
        return ImmutableMap.of(type, object);
    }
}
