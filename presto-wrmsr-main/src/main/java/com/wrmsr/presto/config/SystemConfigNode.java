package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import jdk.nashorn.internal.ir.annotations.Immutable;

import java.util.List;

@Immutable
public class SystemConfigNode
    extends StringListConfigNode
{
    @JsonCreator
    public static SystemConfigNode valueOf(List<String> items)
    {
        return new SystemConfigNode(items);
    }

    public SystemConfigNode(List<String> items)
    {
        super(items);
    }
}
