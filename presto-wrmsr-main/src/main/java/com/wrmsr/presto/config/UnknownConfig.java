package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.merging.UnknownMergingConfigNode;

public final class UnknownConfig
    extends UnknownMergingConfigNode<UnknownConfig> implements MainConfigNode<UnknownConfig>
{
    @JsonCreator
    public UnknownConfig(String type, Object object)
    {
        super(type, object);
    }
}
