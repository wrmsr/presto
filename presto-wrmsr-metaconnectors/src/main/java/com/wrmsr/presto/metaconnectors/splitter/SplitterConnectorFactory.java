package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.NodeManager;

import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

public class SplitterConnectorFactory implements ConnectorFactory
{
    private final ConnectorManager connectorManager;
    private final NodeManager nodeManager;
    private final int defaultSplitsPerNode;

    public SplitterConnectorFactory(NodeManager nodeManager)
    {
        this(nodeManager, Runtime.getRuntime().availableProcessors());
    }

    public SplitterConnectorFactory(NodeManager nodeManager, int defaultSplitsPerNode)
    {
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
        final int splitsPerNode = getSplitsPerNode(properties);

        return new Connector() {
            @Override
            public ConnectorMetadata getMetadata()
            {
                return new SplitterMetadata(connectorId);
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new SplitterSplitManager(connectorId, nodeManager, splitsPerNode);
            }

            @Override
            public ConnectorHandleResolver getHandleResolver()
            {
                return new SplitterHandleResolver(connectorId);
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new SplitterRecordSetProvider();
            }
        };
    }

    private int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return Integer.parseInt(firstNonNull(properties.get("tpch.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property tpch.splits-per-node");
        }
    }

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
}
