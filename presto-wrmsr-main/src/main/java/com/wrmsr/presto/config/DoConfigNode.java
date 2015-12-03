package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;

// TODO sxec / file / script
public class DoConfigNode
    extends StringListConfigNode
{
    @JsonCreator
    public DoConfigNode(List<String> items)
    {
        super(items);
    }
}
