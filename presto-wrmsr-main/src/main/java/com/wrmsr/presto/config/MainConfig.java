package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.scripting.ScriptingConfig;
import com.wrmsr.presto.util.Serialization;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.Serialization.roundTrip;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

public class MainConfig
{
    // FIXME http://stackoverflow.com/a/33637156 ? would have to centralize mapper, would have to special case plugin loading cuz lol
    @JsonCreator
    public static MainConfig valueOf(List<Map<String, Object>> contents)
    {
        ObjectMapper mapper = OBJECT_MAPPER.get();
        Map<String, Class<?>> nodeTypeMap = Serialization.getJsonSubtypeMap(mapper, ConfigNode.class);
        ImmutableList.Builder<ConfigNode> builder = ImmutableList.builder();
        for (Map<String, Object> entryMap : contents) {
            for (Map.Entry<String, Object> entry : entryMap.entrySet()) {
                ConfigNode node;
                if (nodeTypeMap.containsKey(entry.getKey())) {
                    node = roundTrip(OBJECT_MAPPER.get(), ImmutableMap.of(entry.getKey(), entry.getValue()), new TypeReference<ConfigNode>() {});
                }
                else {
                    node = new UnknownConfigNode(entry.getKey(), entry.getValue());
                }
                builder.add(node);
            }
        }
        return new MainConfig(builder.build());
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
