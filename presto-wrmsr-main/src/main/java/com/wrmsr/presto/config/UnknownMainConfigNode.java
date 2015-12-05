package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.config.merging.UnknownMergingConfigNode;

public final class UnknownMainConfigNode
    extends UnknownMergingConfigNode implements MainConfigNode
{
    @JsonCreator
    public UnknownMainConfigNode(String type, Object object)
    {
        super(type, object);
    }
}
