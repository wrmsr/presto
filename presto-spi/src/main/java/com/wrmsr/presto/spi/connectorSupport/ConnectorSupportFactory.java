package com.wrmsr.presto.spi.connectorSupport;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorSession;

import java.util.Optional;

public interface ConnectorSupportFactory
{
    <T extends ConnectorSupport> Optional<T> getConnectorSupport(Class<T> cls, ConnectorSession cs, Connector c);
}
