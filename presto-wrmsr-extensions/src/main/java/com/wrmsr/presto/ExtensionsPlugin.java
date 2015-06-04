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
package com.wrmsr.presto;

// import com.facebook.presto.metadata.FunctionFactory;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.server.ServerEvent;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.ffi.FFIFunctionFactory;
import com.wrmsr.presto.flat.FlatConnectorFactory;
import com.wrmsr.presto.flat.FlatModule;
import com.wrmsr.presto.jdbc.ExtendedJdbcConnectorFactory;
import com.wrmsr.presto.metaconnectors.partitioner.PartitionerConnectorFactory;
import com.wrmsr.presto.metaconnectors.partitioner.PartitionerModule;
import com.wrmsr.presto.mysql.ExtendedMySqlClientModule;
import com.wrmsr.presto.postgresql.ExtendedPostgreSqlClientModule;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

// import com.facebook.presto.type.ParametricType;

public class ExtensionsPlugin
        implements Plugin, ServerEvent.Listener
{
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private ConnectorManager connectorManager;
    private TypeManager typeManager;
    private NodeManager nodeManager;

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Inject
    public void setConnectorManager(ConnectorManager connectorManager)
    {
        this.connectorManager = checkNotNull(connectorManager);
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
    public void onServerEvent(ServerEvent event)
    {
        if (event instanceof ServerEvent.ConnectorsLoaded) {
            if (connectorManager != null) {

            }
        }
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(
                    type.cast(new PartitionerConnectorFactory(optionalConfig, new PartitionerModule(null), getClassLoader(), connectorManager)),
                    type.cast(new FlatConnectorFactory(optionalConfig, new FlatModule(), getClassLoader())),
                    type.cast(new ExtendedJdbcConnectorFactory("extended-mysql", new ExtendedMySqlClientModule(), optionalConfig, getClassLoader())),
                    type.cast(new ExtendedJdbcConnectorFactory("extended-postgresql", new ExtendedPostgreSqlClientModule(), optionalConfig, getClassLoader()))
            );
        }
        else if (type == FunctionFactory.class) {
            return ImmutableList.of(type.cast(new FFIFunctionFactory(typeManager)));
        }
        else if (type == ServerEvent.Listener.class) {
            return ImmutableList.of(type.cast(this));
        }
        return ImmutableList.of();
    }

    private static ClassLoader getClassLoader()
    {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), ExtensionsPlugin.class.getClassLoader());
    }
}
