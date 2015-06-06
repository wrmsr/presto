package com.wrmsr.presto.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.MapConfiguration;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;

public class Configs
{
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
}
