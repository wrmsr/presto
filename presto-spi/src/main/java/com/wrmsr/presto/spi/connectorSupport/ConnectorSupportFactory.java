package com.wrmsr.presto.spi.connectorSupport;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.ConnectorSession;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface ConnectorSupportFactory
{
    default <T extends ConnectorSupport> Optional<T> getConnectorSupport(Class<T> connectorSupportClass, ConnectorSession connectorSession, Connector connector)
    {
        return Optional.empty();
    }

    default <T extends ConnectorSupport> Optional<T> getLegacyConnectorSupport(Class<T> connectorSupportClass, ConnectorSession connectorSession, Connector connector, com.facebook.presto.spi.Connector legacyConnector)
    {
        return Optional.empty();
    }

    final class Default
        implements ConnectorSupportFactory
    {
        private final Class<? extends ConnectorSupport> connectorSupportClass;

        @FunctionalInterface
        public interface Constructor
        {
            ConnectorSupport construct(ConnectorSession connectorSession, Connector connector);
        }

        private final Class<? extends Connector> connectorClass;
        private final Constructor connectorSupportConstructor;

        public Default(Class<? extends ConnectorSupport> connectorSupportClass, Class<? extends Connector> connectorClass, Constructor connectorSupportConstructor)
        {
            this.connectorSupportClass = requireNonNull(connectorSupportClass);
            this.connectorClass = requireNonNull(connectorClass);
            this.connectorSupportConstructor = requireNonNull(connectorSupportConstructor);
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

    final class LegacyDefault
            implements ConnectorSupportFactory
    {
        private final Class<? extends ConnectorSupport> connectorSupportClass;

        @FunctionalInterface
        public interface LegacyConstructor
        {
            ConnectorSupport construct(ConnectorSession connectorSession, Connector connector, com.facebook.presto.spi.Connector legacyConnector);
        }

        private final Class<? extends com.facebook.presto.spi.Connector> legacyConnectorClass;
        private final LegacyConstructor legacyConnectorSupportConstructor;

        public LegacyDefault(Class<? extends ConnectorSupport> connectorSupportClass, Class<? extends com.facebook.presto.spi.Connector> legacyConnectorClass, LegacyConstructor legacyConnectorSupportConstructor)
        {
            this.connectorSupportClass = requireNonNull(connectorSupportClass);
            this.legacyConnectorClass = requireNonNull(legacyConnectorClass);
            this.legacyConnectorSupportConstructor = requireNonNull(legacyConnectorSupportConstructor);
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public <T extends ConnectorSupport> Optional<T> getLegacyConnectorSupport(Class<T> connectorSupportClass, ConnectorSession connectorSession, Connector connector, com.facebook.presto.spi.Connector legacyConnector)
        {
            if (legacyConnectorClass.isInstance(connector) && this.connectorSupportClass.isAssignableFrom(connectorSupportClass)) {
                return Optional.of((T) legacyConnectorSupportConstructor.construct(connectorSession, connector, legacyConnector));
            }
            else {
                return Optional.empty();
            }
        }
    }
}
