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
package com.wrmsr.presto.launcher;

import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import com.wrmsr.presto.util.Serialization;
import com.wrmsr.presto.util.config.Configs;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;
import static com.wrmsr.presto.util.collect.Maps.transformKeys;

public class LauncherConfigs
{
    public static File DEFAULT_CONFIG_FILE = new File("presto.yaml");

    public static final String CONFIG_PROPERTIES_PREFIX = "com.wrmsr.presto.";

    public static ConfigContainer stripConfigFromProperties()
    {
        Map<String, String> configMap = System.getProperties().entrySet().stream()
                .filter(e -> e.getKey() instanceof String && ((String) e.getKey()).startsWith(CONFIG_PROPERTIES_PREFIX) && e.getValue() instanceof String)
                .map(e -> ImmutablePair.of((String) e.getKey(), (String) e.getValue()))
                .collect(toImmutableMap());
        if (configMap.isEmpty()) {
            return new ConfigContainer();
        }
        configMap.keySet().forEach(System::clearProperty);
        HierarchicalConfiguration hierarchicalConfig = Configs.CONFIG_PROPERTIES_CODEC.decode(transformKeys(configMap, k -> {
            String r = k.substring(CONFIG_PROPERTIES_PREFIX.length());
            return r.startsWith("(") ? r : "(0)." + r;
        }));
        return Configs.OBJECT_CONFIG_CODEC.decode(hierarchicalConfig, ConfigContainer.class);
    }

    public static ConfigContainer loadConfig(List<String> filePaths)
    {
        List<File> files;
        if (filePaths.isEmpty()) {
            if (!DEFAULT_CONFIG_FILE.exists()) {
                files = ImmutableList.of();
            }
            else {
                files = ImmutableList.of(DEFAULT_CONFIG_FILE);
            }
        }
        else {
            files = filePaths.stream().map(File::new).collect(toImmutableList());
        }
        ConfigContainer config = new ConfigContainer();
        for (File file : files) {
            ConfigContainer fileConfig = Serialization.readFile(file, ConfigContainer.class);
            config = (ConfigContainer) config.merge(fileConfig);
        }

        ConfigContainer propertiesConfig = stripConfigFromProperties();
        config = (ConfigContainer) config.merge(propertiesConfig);

        Object objectConfig = Serialization.roundTrip(OBJECT_MAPPER.get(), config, Object.class);
        HierarchicalConfiguration hierarchicalConfig = Configs.OBJECT_CONFIG_CODEC.encode(objectConfig);
        Map<String, String> flatConfig = Configs.CONFIG_PROPERTIES_CODEC.encode(hierarchicalConfig);
        Map<String, String> configMap = transformKeys(flatConfig, k -> CONFIG_PROPERTIES_PREFIX + k);
        configMap.entrySet().stream().forEach(e -> System.setProperty(e.getKey(), e.getValue()));

        return config;
    }
}
