/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.util.config;

import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.util.Mergeable;
import com.wrmsr.presto.util.Serialization;
import com.wrmsr.presto.util.config.mergeable.MergeableConfigContainer;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.wrmsr.presto.util.Jvm.getJarFile;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMultimap;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMultiset;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableSet;
import static com.wrmsr.presto.util.collect.Maps.transformKeys;

public class PrestoConfigs
{
    public static File DEFAULT_CONFIG_FILE = new File("presto.yaml");

    public static final String CONFIG_CONFIGURED_PROPERTY = "com.wrmsr.presto.configured";
    public static final String CONFIG_PROPERTIES_PREFIX = "com.wrmsr.presto.config.";

    public static void setConfigItem(String key, String value)
    {
        System.setProperty(CONFIG_PROPERTIES_PREFIX + key, value);
    }

    @SuppressWarnings({"unchecked"})
    public static <T extends MergeableConfigContainer<?>> T stripConfigFromProperties(Class<T> cls)
    {
        Map<String, String> configMap = System.getProperties().entrySet().stream()
                .filter(e -> e.getKey() instanceof String && ((String) e.getKey()).startsWith(CONFIG_PROPERTIES_PREFIX) && e.getValue() instanceof String)
                .map(e -> ImmutablePair.of((String) e.getKey(), (String) e.getValue()))
                .collect(toImmutableMap());
        if (configMap.isEmpty()) {
            return (T) Mergeable.unit(cls);
        }
        configMap.keySet().forEach(System::clearProperty);
        HierarchicalConfiguration hierarchicalConfig = Configs.CONFIG_PROPERTIES_CODEC.decode(transformKeys(configMap, k -> {
            String r = k.substring(CONFIG_PROPERTIES_PREFIX.length());
            return r.startsWith("(") ? r : "(0)." + r;
        }));
        return Configs.OBJECT_CONFIG_CODEC.decode(hierarchicalConfig, cls);
    }

    @SuppressWarnings({"unchecked"})
    public static <T extends MergeableConfigContainer<?>> T loadConfig(Class<?> caller, Class<T> cls, List<String> filePaths)
    {
        List<File> files;
        if (filePaths.isEmpty()) {
            files = ImmutableList.of();
            if (DEFAULT_CONFIG_FILE.exists()) {
                files = ImmutableList.of(DEFAULT_CONFIG_FILE);
            }
            else {
                File jarFile = getJarFile(caller);
                File file = new File(jarFile.getParentFile(), DEFAULT_CONFIG_FILE.getPath());
                if (file.exists()) {
                    files = ImmutableList.of(file);
                }
            }
        }
        else {
            files = filePaths.stream().map(File::new).collect(toImmutableList());
        }
        T config = (T) Mergeable.unit(cls);
        for (File file : files) {
            T fileConfig = Serialization.readFile(file, cls);
            config = (T) config.merge(fileConfig);
        }

        T propertiesConfig = stripConfigFromProperties(cls);
        config = (T) config.merge(propertiesConfig);

        return config;
    }

    public static Map<String, String> configToProperties(Object config)
    {
        Object objectConfig = Serialization.roundTrip(OBJECT_MAPPER.get(), config, Object.class);
        HierarchicalConfiguration hierarchicalConfig = Configs.OBJECT_CONFIG_CODEC.encode(objectConfig);
        Map<String, String> flatConfig = Configs.CONFIG_PROPERTIES_CODEC.encode(hierarchicalConfig);
        flatConfig.keySet().stream().map(k -> k.split("\\.")).map(a -> ImmutablePair.of(a[0], a[1].split("\\(")[0])).collect(toImmutableSet()).stream().collect(toImmutableMap());
        Map<String, String> configMap = transformKeys(flatConfig, k -> CONFIG_PROPERTIES_PREFIX + k);
        return configMap;
    }

    public static <T extends MergeableConfigContainer<?>> T rewriteConfig(Map<String, String> configMap, Function<String, String> rewriter)
    {
        return configMap.entrySet().stream().map(e -> ImmutablePair.of(e.getKey(), rewriter.apply(e.getValue()))).collect(toImmutableMap());
    }

    public static <T extends MergeableConfigContainer<?>> T rewriteConfig(Object config, Function<String, String> rewriter, Class<T> cls)
    {
        Map<String, String> configMap = configToProperties(config);
        Map<String, String> rewrittenConfigMap = configMap.entrySet().stream().map(e -> ImmutablePair.of(e.getKey(), rewriter.apply(e.getValue()))).collect(toImmutableMap());
        return 
    }

    public static void writeConfigProperties(Object config, Function<String, String> rewriter)
    {
        Map<String, String> configMap = configToProperties(config);
        Map<String, String> rewrittenConfigMap = configMap.entrySet().stream().map(e -> ImmutablePair.of(e.getKey(), rewriter.apply(e.getValue()))).collect(toImmutableMap());
        rewrittenConfigMap.entrySet().stream().forEach(e -> System.setProperty(e.getKey(), e.getValue()));
        System.setProperty(CONFIG_CONFIGURED_PROPERTY, "true");
    }

    public static void writeConfigProperties(Object config)
    {
        writeConfigProperties(config, s -> s);
    }

    public static <T extends MergeableConfigContainer<?>> T loadConfigFromProperties(Class<T> cls)
    {
        return loadConfigFromProperties(cls, System.getProperties());
    }

    public static <T extends MergeableConfigContainer<?>> T loadConfigFromProperties(Class<T> cls, Map<Object, Object> properties)
    {
        if (!Boolean.valueOf((String) properties.get(CONFIG_CONFIGURED_PROPERTY))) {
            throw new IllegalStateException("Properties not configured");
        }
        Map<String, String> configMap = properties.entrySet().stream()
                .filter(e -> e.getKey() instanceof String && ((String) e.getKey()).startsWith(CONFIG_PROPERTIES_PREFIX) && e.getValue() instanceof String)
                .map(e -> ImmutablePair.of(((String) e.getKey()).substring(CONFIG_PROPERTIES_PREFIX.length()), (String) e.getValue()))
                .collect(toImmutableMap());
        if (configMap.isEmpty()) {
            return (T) Mergeable.unit(cls);
        }
        HierarchicalConfiguration hierarchicalConfig = Configs.CONFIG_PROPERTIES_CODEC.decode(configMap);
        return Configs.OBJECT_CONFIG_CODEC.decode(hierarchicalConfig, cls);
    }
}
