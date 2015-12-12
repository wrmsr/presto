package com.wrmsr.presto.launcher.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.wrmsr.presto.util.config.mergeable.UnknownMergeableConfigNode;

public final class UnknownConfig
        extends UnknownMergeableConfigNode<UnknownConfig>
        implements LauncherConfigContainer.Node<UnknownConfig>
{
    @JsonCreator
    public UnknownConfig(String type, Object object)
    {
        super(type, object);
    }
}
