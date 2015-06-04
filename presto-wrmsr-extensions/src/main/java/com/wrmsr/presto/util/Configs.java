package com.wrmsr.presto.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.MapConfiguration;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class Configs
{
    public static Map<String, String> stripSubconfig(Map<String, String> properties, String prefix)
    {
        HierarchicalConfiguration hierarchicalProperties = ConfigurationUtils.convertToHierarchical(new MapConfiguration(properties));
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
}
