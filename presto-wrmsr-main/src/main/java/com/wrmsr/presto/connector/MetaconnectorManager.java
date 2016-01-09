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
package com.wrmsr.presto.connector;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Inject;
import com.wrmsr.presto.util.config.Configs;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;

public class MetaconnectorManager
{
    private final ConnectorManager connectorManager;
    private final Map<String, MetaconnectorFactory> metaconnectorFactories;

    @Inject
    public MetaconnectorManager(
            ConnectorManager connectorManager,
            Set<MetaconnectorFactory> metaconnectorFactories)
    {
        this.connectorManager = checkNotNull(connectorManager);
        this.metaconnectorFactories = metaconnectorFactories.stream().map(f -> ImmutablePair.of(f.getName(), f)).collect(toImmutableMap());
    }

    public synchronized void addMetaconnector(String connectorName, Map<String, String> config)
    {
        ConnectorFactory factory = createMetaconnector(connectorName, config);
        connectorManager.addConnectorFactory(factory);
    }

    private ConnectorFactory createMetaconnector(String connectorName, Map<String, String> config)
    {
        config = new HashMap<>(config);
        String name = checkNotNull(config.remove("metaconnector.name"));
        String targetName = checkNotNull(config.remove("target-connector.name"));
        Map<String, String> targetProperties = new HashMap<>(Configs.stripSubconfig(config, "target"));
        MetaconnectorFactory factory = metaconnectorFactories.get(name);

        ConnectorFactory target;
        if (metaconnectorFactories.containsKey(targetName)) {
            targetProperties.put("metaconnector.name", targetName);
            target = createMetaconnector(connectorName + "$target", targetProperties);
        }
        else {
            checkArgument(targetProperties.isEmpty());
            target = connectorManager.getConnectorFactories().get(targetName);
        }

        return factory.create(connectorName, config, target);
    }
}
