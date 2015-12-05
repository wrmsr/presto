package com.wrmsr.presto.util.config.merging;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;

public abstract class IncludeMergingConfigNode
    extends StringListMergingConfigNode
{
    /*
    @JsonCreator
    public static IncludeMergingConfigNode valueOf(Object object)
    {
        return new IncludeMergingConfigNode(unpack(object, String.class));
    }
    */

    public IncludeMergingConfigNode(List<String> items)
    {
        super(items);
    }
}
