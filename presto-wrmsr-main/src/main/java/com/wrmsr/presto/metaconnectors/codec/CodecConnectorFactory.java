package com.wrmsr.presto.metaconnectors.codec;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.spi.Connector;
import com.google.inject.Module;
import com.wrmsr.presto.metaconnectors.MetaconnectorConnectorFactory;

import java.util.Map;

public class CodecConnectorFactory
        extends MetaconnectorConnectorFactory
{
    public CodecConnectorFactory(Map<String, String> optionalConfig, Module module, ClassLoader classLoader, ConnectorManager connectorManager)
    {
        super(optionalConfig, module, classLoader, connectorManager);
    }

    @Override
    public String getName()
    {
        return "codec";
    }

    @Override
    public Connector create(Connector target, String connectorId, Map<String, String> requiredConfiguration)
    {
        return null;
    }
}
