package com.wrmsr.presto.spi;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorSession;

import java.util.Optional;

public interface ConnectorSupportFactory
{
    Optional<ConnectorSupport> getConnectorSupport(ConnectorSession cs, Connector c);
}
