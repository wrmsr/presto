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
package com.wrmsr.presto.metaconnectors;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import com.wrmsr.presto.metaconnectors.partitioner.PartitionerConnectorFactory;
import com.wrmsr.presto.metaconnectors.partitioner.PartitionerModule;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.MoreObjects.firstNonNull;

public class MetaconnectorsPlugin
        implements Plugin
{
    private ConnectorManager connectorManager;
    private TypeManager typeManager;
    private NodeManager nodeManager;
    private Map<String, String> optionalConfig = ImmutableMap.of();

    @Inject
    public void setConnectorManager(ConnectorManager connectorManager)
    {
        this.connectorManager = connectorManager;
    }

    @Inject
    public void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    @Inject
    public void setNodeManager(NodeManager nodeManager)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = optionalConfig;
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            Module module = new PartitionerModule(null);
            return ImmutableList.of(type.cast(new PartitionerConnectorFactory(optionalConfig, module, getClassLoader(), connectorManager)));
        }
//        if (type == FunctionFactory.class) {
//            return ImmutableList.of(type.cast(new MLFunctionFactory(typeManager)));
//        }
//        else if (type == Type.class) {
//            return ImmutableList.of(type.cast(MODEL), type.cast(REGRESSOR));
//        }
//        else if (type == ParametricType.class) {
//            return ImmutableList.of(type.cast(new ClassifierParametricType()));
//        }
        return ImmutableList.of();
    }

    private static ClassLoader getClassLoader()
    {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), MetaconnectorsPlugin.class.getClassLoader());
    }
}
