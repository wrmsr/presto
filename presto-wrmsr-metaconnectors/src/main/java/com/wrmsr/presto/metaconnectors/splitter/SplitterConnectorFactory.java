package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.NodeManager;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.metaconnectors.util.ImmutableCollectors;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.MapConfiguration;

import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

public class SplitterConnectorFactory implements ConnectorFactory
{
    private final ConnectorManager connectorManager;
    private final NodeManager nodeManager;
    private final int defaultSplitsPerNode;

    public SplitterConnectorFactory(ConnectorManager connectorManager, NodeManager nodeManager)
    {
        this(connectorManager, nodeManager, Runtime.getRuntime().availableProcessors());
    }

    public SplitterConnectorFactory(ConnectorManager connectorManager, NodeManager nodeManager, int defaultSplitsPerNode)
    {
        this.connectorManager = checkNotNull(connectorManager, "connectorManager");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.defaultSplitsPerNode = defaultSplitsPerNode;
    }

    @Override
    public String getName()
    {
        return "splitter";
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> properties)
    {
        String targetCatalogName = checkNotNull(properties.get("target-name"));
        String targetConnectorName = properties.get("target-connector-name");

        final Connector target;

        if (targetConnectorName == null) {
            target = checkNotNull(connectorManager.getConnectors().get(targetCatalogName), "target-connector-name not specified and target not found");

        } else {
            HierarchicalConfiguration hierarchicalProperties = ConfigurationUtils.convertToHierarchical(
                    new MapConfiguration(properties));
            Map<String, String> targetProperties;
            final Configuration targetConfiguration;
            try {
                targetConfiguration = hierarchicalProperties.configurationAt("target");
                targetProperties = new ConfigurationMap(targetConfiguration).entrySet().stream()
                        .collect(ImmutableCollectors.toImmutableMap(e -> checkNotNull(e.getKey()).toString(), e -> checkNotNull(e.getValue()).toString()));
            }
            catch (IllegalArgumentException e) {
                targetProperties = ImmutableMap.of();
            }
            connectorManager.createConnection(targetCatalogName, targetConnectorName, targetProperties);
            target = checkNotNull(connectorManager.getConnectors().get(targetCatalogName));
        }

        final int splitsPerNode = getSplitsPerNode(properties);

        return new Connector() {
            @Override
            public ConnectorMetadata getMetadata()
            {
                return target.getMetadata();
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new SplitterSplitManager(connectorId, target.getSplitManager(), target, nodeManager, splitsPerNode);
            }

            @Override
            public ConnectorHandleResolver getHandleResolver()
            {
                return new SplitterHandleResolver(connectorId, target.getHandleResolver());
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new SplitterRecordSetProvider(target.getRecordSetProvider());
            }
        };
    }

    private int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return Integer.parseInt(firstNonNull(properties.get("splitter.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property splitter.splits-per-node");
        }
    }

    /*
    private void loadCatalog(File file)
            throws Exception
    {
        log.info("-- Loading catalog %s --", file);
        Map<String, String> properties = new HashMap<>(loadProperties(file));

        String connectorName = properties.remove("connector.name");
        checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());

        String catalogName = Files.getNameWithoutExtension(file.getName());

        connectorManager.createConnection(catalogName, connectorName, ImmutableMap.copyOf(properties));
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }
    */
}
