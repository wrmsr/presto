package com.wrmsr.presto.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.configuration.tree.ConfigurationNodeVisitorAdapter;
import org.apache.commons.configuration.tree.DefaultConfigurationKey;
import org.apache.commons.configuration.tree.DefaultConfigurationNode;
import org.apache.commons.configuration.tree.DefaultExpressionEngine;
import org.apache.commons.configuration.tree.NodeAddData;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.transformValues;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPERS_BY_EXTENSION;
import static com.wrmsr.presto.util.Primitives.toBool;

public class Configs
{
    private Configs()
    {
    }

    public static HierarchicalConfiguration toHierarchical(Map<String, String> properties)
    {
        return toHierarchical(new MapConfiguration(properties));
    }

    public static final String IS_LIST_ATTR = "__is_list__";

    public static class ListAnnotatingHierarchicalConfiguration extends HierarchicalConfiguration
    {
        public ListAnnotatingHierarchicalConfiguration()
        {
            checkArgument(getExpressionEngine() instanceof DefaultExpressionEngine);
        }

        public ListAnnotatingHierarchicalConfiguration(HierarchicalConfiguration c)
        {
            super(c);
            checkArgument(getExpressionEngine() instanceof DefaultExpressionEngine);
        }

        @Override
        protected void addPropertyDirect(String key, Object obj)
        {
            if (key.endsWith("[@" + IS_LIST_ATTR + "]") && containsKey(key)) {
                return;
            }

            @SuppressWarnings({"unchecked"})
            DefaultExpressionEngine engine = (DefaultExpressionEngine) getExpressionEngine();
            DefaultConfigurationKey engineKey = new DefaultConfigurationKey(engine, key);
            DefaultConfigurationKey.KeyIterator keyIt = engineKey.iterator();
            while (keyIt.hasNext()) {
                keyIt.nextKey(false);
            }

            NodeAddData data = getExpressionEngine().prepareAdd(getRootNode(), key);
            ConfigurationNode node = processNodeAddData(data);
            node.setValue(obj);

            if (keyIt.hasIndex() && !(keyIt.isAttribute() && IS_LIST_ATTR.equals(keyIt.currentKey()))) {
                // FIXME jesus.
                String isListKey = key.substring(0, key.lastIndexOf("(")) + "[@" + IS_LIST_ATTR + "]";
                addPropertyDirect(isListKey, true);
            }
        }

        private ConfigurationNode processNodeAddData(NodeAddData data)
        {
            ConfigurationNode node = data.getParent();

            // Create missing nodes on the path
            for (String name : data.getPathNodes()) {
                ConfigurationNode child = createNode(name);
                node.addChild(child);
                node = child;
            }

            // Add new target node
            ConfigurationNode child = createNode(data.getNewNodeName());
            if (data.isAttribute()) {
                node.addAttribute(child);
            }
            else {
                node.addChild(child);
            }
            return child;
        }
    }

    public static HierarchicalConfiguration toHierarchical(Configuration conf)
    {
        if (conf == null) {
            return null;
        }

        if (conf instanceof HierarchicalConfiguration) {
            HierarchicalConfiguration hc = (HierarchicalConfiguration) conf;
            checkArgument(hc.getExpressionEngine() instanceof DefaultExpressionEngine);
            return hc;
        }
        else {
            HierarchicalConfiguration hc = new ListAnnotatingHierarchicalConfiguration();

            // Workaround for problem with copy()
            boolean delimiterParsingStatus = hc.isDelimiterParsingDisabled();
            hc.setDelimiterParsingDisabled(true);
            hc.append(conf);
            hc.setDelimiterParsingDisabled(delimiterParsingStatus);
            return hc;
        }
    }

    @SuppressWarnings({"unchecked"})
    public static Map<String, Object> unpackHierarchical(HierarchicalConfiguration config)
    {
        return (Map<String, Object>) unpackNode(config.getRootNode(), false);
    }

    public static Object unpackNode(ConfigurationNode node, boolean ignoreListAttr)
    {
        List<ConfigurationNode> children = node.getChildren();
        if (!children.isEmpty()) {
            Map<String, List<ConfigurationNode>> namedChildren = newHashMap();
            children.forEach(child -> {
                if (namedChildren.containsKey(child.getName())) {
                    namedChildren.get(child.getName()).add(child);
                }
                else {
                    namedChildren.put(child.getName(), newArrayList(child));
                }
            });
            return newHashMap(transformValues(namedChildren, l -> {
                checkState(!l.isEmpty());
                if (l.size() > 1) {
                    return l.stream().map(n ->  unpackNode(n, true)).collect(ImmutableCollectors.toImmutableList());
                }
                else {
                    return unpackNode(l.get(0), false);
                }
            }));
        }
        if (!ignoreListAttr) {
            List<ConfigurationNode> isListAtts = node.getAttributes(IS_LIST_ATTR);
            boolean isList = isListAtts.stream().map(o -> toBool(o)).findFirst().isPresent();
            if (isList) {
                return ImmutableList.of(unpackNode(node, true));
            }
        }
        return node.getValue();
    }

    public static Map<String, String> stripSubconfig(Map<String, String> properties, String prefix)
    {
        HierarchicalConfiguration hierarchicalProperties = toHierarchical(properties);
        Configuration subconfig;
        try {
            subconfig = hierarchicalProperties.configurationAt(prefix);
        }
        catch (IllegalArgumentException e) {
            return ImmutableMap.of();
        }

        Map<String, String> subproperties = new ConfigurationMap(subconfig).entrySet().stream()
                .collect(ImmutableCollectors.toImmutableMap(e -> checkNotNull(e.getKey()).toString(), e -> checkNotNull(e.getValue()).toString()));

        hierarchicalProperties.clearTree(prefix);
        for (String key : Sets.newHashSet(properties.keySet())) {
            if (!hierarchicalProperties.containsKey(key)) {
                properties.remove(key);
            }
        }

        return subproperties;
    }

    public static List<String> getAllStrings(HierarchicalConfiguration hc, String key)
    {
        List<String> values = hc.getList(key).stream().filter(o -> o != null).map(Object::toString).collect(Collectors.toList());
        try {
            HierarchicalConfiguration subhc = hc.configurationAt(key);
            for (String subkey : newArrayList(subhc.getKeys())) {
                if (!isNullOrEmpty(subkey)) {
                    String subvalue = subhc.getString(subkey);
                    if (subvalue != null) {
                        values.add(subvalue);
                    }
                }
            }
        }
        catch (IllegalArgumentException e) {
            // pass
        }
        return values;
    }

    @SuppressWarnings({"unchecked"})
    public static Map<String, String> flattenValues(String key, Object value)
    {
        /*
        TODO:
        !!binary	byte[], String
        !!timestamp	java.util.Date, java.sql.Date, java.sql.Timestamp
        !!omap, !!pairs	List of Object[]
        */
        if (value == null) {
            Map<String, String> map = newHashMap();
            map.put(key, null);
            return map;
        }
        else if (value instanceof Map) {
            Map<String, Object> mapValue = (Map<String, Object>) value;
            return mapValue.entrySet().stream()
                    .flatMap(e -> flattenValues((key != null ? key + "." : "") + e.getKey(), e.getValue()).entrySet().stream())
                    .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
        }
        else if (value instanceof List || value instanceof Set) {
            List<Object> listValue = ImmutableList.copyOf((Iterable<Object>) value);
            return IntStream.range(0, listValue.size()).boxed()
                    .flatMap(i -> {
                        String subkey = key + "(" + i.toString() + ")"; // FIXME expressionengine
                        return flattenValues(subkey, listValue.get(i)).entrySet().stream();
                    })
                    .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
        }
        else {
            return ImmutableMap.of(key, value.toString());
        }
    }

    public static Map<String, String> flattenValues(Object value)
    {
        return flattenValues(null, value);
    }

    public static Map<String, String> loadByExtension(byte[] data, String extension)
    {
        if (extension == "properties") {
            Properties properties = new Properties();
            try {
                properties.load(new StringReader(new String(data)));
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return properties.entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
        }
        else if (OBJECT_MAPPERS_BY_EXTENSION.containsKey(extension)) {
            ObjectMapper objectMapper = OBJECT_MAPPERS_BY_EXTENSION.get(extension).get();
            Object value;
            try {
                value = objectMapper.readValue(data, Object.class);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return flattenValues(value);
        }
        else {
            throw new IllegalArgumentException(String.format("Unhandled config extension: %s", extension));
        }
    }

    // FIXME invert
    public static final Codecs.Codec<Map<String, String>, HierarchicalConfiguration> PROPERTIES_CONFIG_CODEC = Codecs.Codec.of(
            m -> toHierarchical(m),
            hc -> flattenValues(Configs.unpackHierarchical(hc)));

    public static final Codecs.Codec<HierarchicalConfiguration, Object> CONFIG_OBJECT_CODEC = Codecs.Codec.of(
            hc -> unpackHierarchical(hc),
            o -> toHierarchical(flattenValues(o)));
}
