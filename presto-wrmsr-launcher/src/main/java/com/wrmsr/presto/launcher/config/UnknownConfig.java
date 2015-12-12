package com.wrmsr.presto.launcher.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.merging.UnknownMergingConfigNode;

public final class UnknownConfig
        extends UnknownMergingConfigNode<UnknownConfig>
        implements LauncherConfigContainer.Node<UnknownConfig>
{
    @JsonCreator
    public UnknownConfig(String type, Object object)
    {
        super(type, object);
    }
}
