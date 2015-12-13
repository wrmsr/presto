package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.mergeable.UnknownMergeableConfigNode;

public final class UnknownConfig
        extends UnknownMergeableConfigNode<UnknownConfig>
        implements MainConfigNode<UnknownConfig>
{
    @JsonCreator
    public UnknownConfig(String type, Object object)
    {
        super(type, object);
    }
}
