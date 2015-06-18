package com.wrmsr.presto.metaconnectors.codec;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;

import java.util.Map;

public class CodecConnectorFactory implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return null;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        return null;
    }
}
