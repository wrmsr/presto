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
package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.spi.Connector;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.wrmsr.presto.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.jdbc.ExtendedJdbcConnector;
import com.wrmsr.presto.jdbc.JdbcPartitioner;
import com.wrmsr.presto.metaconnectors.MetaconnectorConnectorFactory;

import java.util.Map;

import static com.wrmsr.presto.util.Exceptions.runtimeThrowing;

public class PartitionerConnectorFactory extends MetaconnectorConnectorFactory
{
    public PartitionerConnectorFactory(Map<String, String> optionalConfig, Module module, ClassLoader classLoader, ConnectorManager connectorManager)
    {
        super(optionalConfig, module, classLoader, connectorManager);
    }

    @Override
    public String getName()
    {
        return "partitioner";
    }

    @Override
    public Connector create(Connector target, String connectorId, Map<String, String> requiredConfiguration)
    {
        Partitioner partitioner = null;
        if (target instanceof ExtendedJdbcConnector) {
            JdbcMetadata jdbcMetadata = (JdbcMetadata) ((ExtendedJdbcConnector) target).getMetadata();
            ExtendedJdbcClient jdbcClient = (ExtendedJdbcClient) jdbcMetadata.getJdbcClient();
            partitioner = new JdbcPartitioner(
                    runtimeThrowing(() -> jdbcClient.getConnection()),
                    jdbcClient::quoted);
        }
        final Partitioner finalPartitioner = partitioner; // FIXME config / explode

        return createWithOverride(
                requiredConfiguration,
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(Partitioner.class).toInstance(finalPartitioner);
                        binder.bind(PartitionerConnectorId.class).toInstance(new PartitionerConnectorId(connectorId));
                        binder.bind(PartitionerTarget.class).toInstance(new PartitionerTarget(target));
                    }
                });
    }
}
