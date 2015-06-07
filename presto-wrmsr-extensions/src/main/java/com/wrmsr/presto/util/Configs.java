package com.wrmsr.presto.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.MapConfiguration;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPERS_BY_EXTENSION;

public class Configs
{
    private Configs()
    {
    }

    public static HierarchicalConfiguration toHierarchical(Map<String, String> properties)
    {
        return ConfigurationUtils.convertToHierarchical(new MapConfiguration(properties));
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
        } catch (IllegalArgumentException e) {
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
                    .flatMap(i -> flattenValues(key + "(" + i.toString() + ")", listValue.get(i)).entrySet().stream())
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
}
