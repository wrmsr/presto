package com.wrmsr.presto.launcher.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import com.wrmsr.presto.util.config.merging.MergingConfig;
import com.wrmsr.presto.util.config.merging.MergingConfigNode;

import java.util.List;
import java.util.Map;

import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

public class LauncherConfigContainer
        extends MergingConfig<LauncherConfigContainer.Node>
{
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.WRAPPER_OBJECT
    )
    @JsonSubTypes({
            @JsonSubTypes.Type(value = LauncherConfig.class, name = "launcher"),
    })
    public interface Node<N extends Node<N>>
        extends MergingConfigNode<N>
    {
    }

    @JsonCreator
    public static LauncherConfigContainer valueOf(List<Map<String, Object>> contents)
    {
        return new LauncherConfigContainer(nodesFrom(OBJECT_MAPPER.get(), contents, Node.class, UnknownConfig::new));
    }

    public LauncherConfigContainer()
    {
        super(Node.class);
    }

    public LauncherConfigContainer(List<Node> nodes)
    {
        super(nodes, Node.class);
    }

    @JsonValue
    public List<Map<String, Object>> jsonValue()
    {
        return jsonValue(OBJECT_MAPPER.get());
    }
}

