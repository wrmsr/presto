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
