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
import org.apache.commons.configuration.tree.ExpressionEngine;
import org.apache.commons.configuration.tree.NodeAddData;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
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
            super.setExpressionEngine(new ListPreservingDefaultExpressionEngine());
        }

        public ListAnnotatingHierarchicalConfiguration(HierarchicalConfiguration c)
        {
            super(c);
            super.setExpressionEngine(new ListPreservingDefaultExpressionEngine());
        }

        @Override
        public void setExpressionEngine(ExpressionEngine expressionEngine)
        {
            throw new IllegalStateException();
        }

        public ListPreservingDefaultExpressionEngine getExpressionEngine()
        {
            return (ListPreservingDefaultExpressionEngine) checkNotNull(super.getExpressionEngine());
        }

        @Override
        protected void addPropertyDirect(String key, Object obj)
        {
            ListPreservingDefaultExpressionEngine.NodeAddData data = getExpressionEngine().prepareAdd(getRootNode(), key);
            ConfigurationNode node = processNodeAddData(data);
            node.setValue(obj);
            for (String k : data.getListAttributes()) {
                String p = k + new ListPreservingDefaultConfigurationKey(getExpressionEngine()).constructAttributeKey(IS_LIST_ATTR);
                if (!containsKey(p)) {
                    addProperty(p, true);
                }
            }
        }

        public void addNodes(String key, Collection<? extends ConfigurationNode> nodes)
        {
            throw new IllegalStateException();

            /*
            if (nodes == null || nodes.isEmpty())
            {
                return;
            }

            fireEvent(EVENT_ADD_NODES, key, nodes, true);
            ConfigurationNode parent;
            List<ConfigurationNode> target = fetchNodeList(key);
            if (target.size() == 1)
            {
                // existing unique key
                parent = target.get(0);
            }
            else
            {
                // otherwise perform an add operation
                parent = processNodeAddData(getExpressionEngine().prepareAdd(
                        getRootNode(), key));
            }

            if (parent.isAttribute())
            {
                throw new IllegalArgumentException(
                        "Cannot add nodes to an attribute node!");
            }

            for (ConfigurationNode child : nodes)
            {
                if (child.isAttribute())
                {
                    parent.addAttribute(child);
                }
                else
                {
                    parent.addChild(child);
                }
                clearReferences(child);
            }
            fireEvent(EVENT_ADD_NODES, key, nodes, false);
            */
        }


        private ConfigurationNode processNodeAddData(NodeAddData data)
        {
            ConfigurationNode node = data.getParent();

            // Create missing nodes on the path
            for (String name : data.getPathNodes())
            {
                ConfigurationNode child = createNode(name);
                node.addChild(child);
                node = child;
            }

            // Add new target node
            ConfigurationNode child = createNode(data.getNewNodeName());
            if (data.isAttribute())
            {
                node.addAttribute(child);
            }
            else
            {
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
        return (Map<String, Object>) unpackNode(config.getRootNode());
    }

    public static Object unpackNode(ConfigurationNode node)
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
                boolean hasListAtt = false;
                ConfigurationNode parent = node.getParentNode();
                if (parent != null) {
                    List<ConfigurationNode> isListAtts = node.getAttributes(IS_LIST_ATTR);
                    hasListAtt = isListAtts.stream().map(o -> toBool(o)).findFirst().isPresent();
                }
                if (l.size() > 1 || hasListAtt) {
                    return l.stream().map(n -> unpackNode(n)).filter(o -> o != null).collect(ImmutableCollectors.toImmutableList());
                }
                else {
                    return unpackNode(l.get(0));
                }
            }));
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

    public static final int LIST_BASE = 0;
    public static final String LIST_START = "(";
    public static final String LIST_END = ")";
    public static final String FIELD_SEPERATOR = ".";

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
                    .flatMap(e -> flattenValues((key != null ? key + FIELD_SEPERATOR : "") + e.getKey(), e.getValue()).entrySet().stream())
                    .filter(e -> e.getValue() != null)
                    .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
        }
        else if (value instanceof List || value instanceof Set) {
            List<Object> listValue = ImmutableList.copyOf((Iterable<Object>) value);
            return IntStream.range(0, listValue.size()).boxed()
                    .flatMap(i -> {
                        String subkey = key + LIST_START + Integer.toString(i + LIST_BASE) + LIST_END; // FIXME expressionengine
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
    public static final Codecs.Codec<HierarchicalConfiguration, Map<String, String>> CONFIG_PROPERTIES_CODEC = Codecs.Codec.of(
            hc -> flattenValues(Configs.unpackHierarchical(hc)),
            m -> toHierarchical(m));

    public static final Codecs.Codec<Object, HierarchicalConfiguration> OBJECT_CONFIG_CODEC = Codecs.Codec.of(
            o -> toHierarchical(flattenValues(o)),
            hc -> unpackHierarchical(hc));
}
