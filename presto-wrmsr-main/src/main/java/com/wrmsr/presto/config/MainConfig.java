package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.wrmsr.presto.util.config.merging.MergingConfig;

import java.util.List;
import java.util.Map;

import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

public class MainConfig
        extends MergingConfig<MainConfigNode>
{
    @JsonCreator
    public static MainConfig valueOf(List<Map<String, Object>> contents)
    {
        return new MainConfig(nodesFrom(OBJECT_MAPPER.get(), contents, MainConfigNode.class, UnknownConfig::new));
    }

    public MainConfig(List<MainConfigNode> nodes)
    {
        super(nodes, MainConfigNode.class);
    }

    @JsonValue
    public List<Map<String, Object>> jsonValue()
    {
        return jsonValue(OBJECT_MAPPER.get());
    }
}
