package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;

public class ExecConfigNode
    extends StringListConfigNode
{
    @JsonCreator
    public ExecConfigNode(List<String> items)
    {
        super(items);
    }
}
