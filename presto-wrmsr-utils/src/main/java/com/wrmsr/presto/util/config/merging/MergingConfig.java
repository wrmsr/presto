package com.wrmsr.presto.util.config.merging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.Serialization;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static com.wrmsr.presto.util.Serialization.roundTrip;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public abstract class MergingConfig<N extends MergingConfigNode>
{
    @FunctionalInterface
    public interface UnknownNodeHandler<N extends MergingConfigNode>
    {
        N handle(String type, Object contents) throws IOException;
    }

    public static <N extends MergingConfigNode> List<N> nodesFrom(ObjectMapper mapper, List<Map<String, Object>> contents, Class<? extends N> ncls)
    {
        return nodesFrom(mapper, contents, ncls, (t, c) -> {
            throw new IllegalArgumentException(t);
        });
    }

    // FIXME http://stackoverflow.com/a/33637156 ? would have to centralize mapper, would have to special case plugin loading cuz lol
    public static <N extends MergingConfigNode> List<N> nodesFrom(ObjectMapper mapper, List<Map<String, Object>> contents, Class<? extends N> ncls, UnknownNodeHandler<N> unh)
    {
        Map<String, Class<?>> nodeTypeMap = Serialization.getJsonSubtypeMap(mapper, ncls);
        ImmutableList.Builder<N> builder = ImmutableList.builder();
        for (Map<String, Object> entryMap : contents) {
            for (Map.Entry<String, Object> entry : entryMap.entrySet()) {
                N node;
                if (nodeTypeMap.containsKey(entry.getKey())) {
                    node = roundTrip(OBJECT_MAPPER.get(), ImmutableMap.of(entry.getKey(), entry.getValue()), ncls);
                }
                else {
                    try {
                        node = unh.handle(entry.getKey(), entry.getValue());
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
                builder.add(node);
            }
        }
        return builder.build();
    }

    public static <N extends MergingConfigNode> List<Map<String, Object>> fromNodes(ObjectMapper mapper, List<N> nodes, Class<? extends N> ncls)
    {
        Map<Class<?>, String> nodeTypeMap = Serialization.getJsonSubtypeMap(mapper, ncls).inverse();
        ImmutableList.Builder<Map<String, Object>> builder = ImmutableList.builder();
        for (N node : nodes) {
            if (node instanceof UnknownMergingConfigNode) {
                builder.add(((UnknownMergingConfigNode) node).jsonValue());
            }
            else {
                String type = nodeTypeMap.get(node.getClass());
                builder.add(ImmutableMap.of(type, roundTrip(mapper, node, Map.class)));
            }
        }
        return builder.build();
    }

    protected final List<N> nodes;
    protected final Class<N> nodeClass;

    public MergingConfig(List<N> nodes, Class<N> nodeClass)
    {
        this.nodes = ImmutableList.copyOf(nodes);
        this.nodeClass = nodeClass;
    }

    public List<Map<String, Object>> jsonValue(ObjectMapper mapper)
    {
        return fromNodes(mapper, nodes, nodeClass);
    }

    public List<N> getNodes()
    {
        return nodes;
    }

    public <T extends MergingConfigNode> List<T> getNodes(Class<T> cls)
    {
        return nodes.stream().filter(cls::isInstance).map(cls::cast).collect(toImmutableList());
    }

    @SuppressWarnings({"unchecked"})
    public <T> List<T> getList(Class<? extends ListMergingConfigNode<T>> cls)
    {
        return getNodes(cls).stream().flatMap(n -> n.getItems().stream()).collect(toImmutableList());
    }

    @SuppressWarnings({"unchecked"})
    public <K, V> Map<K, V> getMap(Class<? extends MapMergingConfigNode<K, V>> cls)
    {
        Map<K, V> map = newHashMap();
        for (MapMergingConfigNode node : getNodes(cls)) {
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
