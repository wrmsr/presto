package com.wrmsr.presto.spi.connectorSupport;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.ConnectorSession;

import java.util.Optional;

public interface ConnectorSupportFactory
{
    <T extends ConnectorSupport> Optional<T> getConnectorSupport(Class<T> connectorSupportClass, ConnectorSession connectorSession, Connector connector);

    final class Default
        implements ConnectorSupportFactory
    {
        @FunctionalInterface
        public interface Constructor
        {
            ConnectorSupport construct(ConnectorSession connectorSession, Connector connector);
        }

        private final Class<? extends Connector> connectorClass;
        private final Class<? extends ConnectorSupport> connectorSupportClass;
        private final Constructor connectorSupportConstructor;

        public Default(Class<? extends Connector> connectorClass, Class<? extends ConnectorSupport> connectorSupportClass, Constructor connectorSupportConstructor)
        {
            this.connectorClass = connectorClass;
            this.connectorSupportClass = connectorSupportClass;
            this.connectorSupportConstructor = connectorSupportConstructor;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public <T extends ConnectorSupport> Optional<T> getConnectorSupport(Class<T> connectorSupportClass, ConnectorSession connectorSession, Connector connector)
        {
            if (connectorClass.isInstance(connector) && this.connectorSupportClass.isAssignableFrom(connectorSupportClass)) {
                return Optional.of((T) connectorSupportConstructor.construct(connectorSession, connector));
            }
            else {
                return Optional.empty();
            }
        }
    }
}
