package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.scripting.ScriptingConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class MainConfig
{
    @JsonCreator
    public static MainConfig valueOf(List<ConfigNode> nodes)
    {
        return new MainConfig(nodes);
    }

    private final List<ConfigNode> nodes;

    public MainConfig(List<ConfigNode> nodes)
    {
        this.nodes = nodes;
    }

    @JsonValue
    public List<ConfigNode> getNodes()
    {
        return nodes;
    }

    public <T extends ConfigNode> List<T> getNodes(Class<T> cls)
    {
        return nodes.stream().filter(cls::isInstance).map(cls::cast).collect(toImmutableList());
    }

    @SuppressWarnings({"unchecked"})
    public <T> List<T> getList(Class<? extends ListConfigNode<T>> cls)
    {
        return getNodes(cls).stream().flatMap(n -> n.getItems().stream()).collect(toImmutableList());
    }

    @SuppressWarnings({"unchecked"})
    public <K, V> Map<K, V> getMap(Class<? extends MapConfigNode<K, V>> cls)
    {
        Map<K, V> map = newHashMap();
        for (MapConfigNode node : getNodes(cls)) {
            Set<Map.Entry<K, V>> entries = node.getEntries().entrySet();
            for (Map.Entry<K, V> entry : entries) {
                if (!map.containsKey(entry.getKey())) {
                    map.put(entry.getKey(), entry.getValue());
                }
            }

        }
        return ImmutableMap.copyOf(map);
    }
}
