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
package com.wrmsr.presto.connectorSupport;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.transaction.LegacyTransactionConnector;
import com.wrmsr.presto.spi.connectorSupport.ConnectorSupport;
import com.wrmsr.presto.spi.connectorSupport.ConnectorSupportFactory;

import java.util.Optional;

public class LegacyConnectorSupportFactory
    implements ConnectorSupportFactory
{
    @FunctionalInterface
    public interface Constructor
    {
        ConnectorSupport construct(ConnectorSession connectorSession, LegacyTransactionConnector wrapperConnector);
    }

    private final Class<? extends com.facebook.presto.spi.Connector> connectorClass;
    private final Class<? extends ConnectorSupport> connectorSupportClass;
    private final Constructor connectorSupportConstructor;

    public LegacyConnectorSupportFactory(Class<? extends com.facebook.presto.spi.Connector> connectorClass, Class<? extends ConnectorSupport> connectorSupportClass, Constructor connectorSupportConstructor)
    {
        this.connectorClass = connectorClass;
        this.connectorSupportClass = connectorSupportClass;
        this.connectorSupportConstructor = connectorSupportConstructor;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public <T extends ConnectorSupport> Optional<T> getConnectorSupport(Class<T> connectorSupportClass, ConnectorSession connectorSession, Connector connector)
    {
        if (!(connector instanceof LegacyTransactionConnector)) {
            return Optional.empty();
        }
        LegacyTransactionConnector wrapperConnectr = (LegacyTransactionConnector) connector;
        com.facebook.presto.spi.Connector legacyConnector = wrapperConnectr.getConnector();
        if (connectorClass.isInstance(legacyConnector) && this.connectorSupportClass.isAssignableFrom(connectorSupportClass)) {
            return Optional.of((T) connectorSupportConstructor.construct(connectorSession, wrapperConnectr));
        }
        else {
            return Optional.empty();
        }
    }
}
