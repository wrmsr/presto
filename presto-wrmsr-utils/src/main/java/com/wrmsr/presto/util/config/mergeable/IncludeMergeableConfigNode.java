package com.wrmsr.presto.util.config.mergeable;

import java.util.List;

public abstract class IncludeMergeableConfigNode<N extends IncludeMergeableConfigNode<N>>
    extends StringListMergeableConfigNode<N>
{
    /*
    @JsonCreator
    public static IncludeMergeableConfigNode valueOf(Object object)
    {
        return new IncludeMergeableConfigNode(unpack(object, String.class));
    }
    */

    public IncludeMergeableConfigNode(List<String> items)
    {
        super(items);
    }
}
