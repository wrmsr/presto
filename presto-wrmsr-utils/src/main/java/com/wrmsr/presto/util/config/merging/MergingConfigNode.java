package com.wrmsr.presto.util.config.merging;

public interface MergingConfigNode<N extends MergingConfigNode>
{
    default N merge(N other)
    {
        throw new UnsupportedOperationException();
    }
}
