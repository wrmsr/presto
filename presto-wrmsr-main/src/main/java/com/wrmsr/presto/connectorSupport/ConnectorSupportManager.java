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

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.ConnectorSession;
import com.wrmsr.presto.spi.connectorSupport.ConnectorSupport;
import com.wrmsr.presto.spi.connectorSupport.ConnectorSupportFactory;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

public class ConnectorSupportManager
{
    private volatile Set<ConnectorSupportFactory> connectorSupportFactories;

    @Inject
    public ConnectorSupportManager(Set<ConnectorSupportFactory> connectorSupportFactories)
    {
        this.connectorSupportFactories = connectorSupportFactories;
    }

    public <T extends ConnectorSupport> Optional<T> getConnectorSupport(Class<T> cls, ConnectorSession connectorSession, Connector connector)
    {
        for (ConnectorSupportFactory f : connectorSupportFactories) {
            Optional<T> cs = f.getConnectorSupport(cls, connectorSession, connector);
            if (cs.isPresent()) {
                return cs;
            }
        }
        return Optional.empty();
    }
//
//    public PkLayout<String> getTableTupleLayout(KeyConnectorSupport t, SchemaTableName schemaTableName)
//    {
//        List<KeyConnectorSupport.Key> keys = t.getKeys(schemaTableName);
//        List<String> pk = t.getPrimaryKey(schemaTableName);
//        ConnectorSession cs = t.getConnectorSession();
//        ConnectorMetadata m = t.getConnector().getMetadata();
//        ConnectorTableHandle th = m.getTableHandle(cs, schemaTableName);
//        List<ColumnHandle> chs = m.getColumnHandles(cs, th).values().stream().collect(toImmutableList());
//        return new PkLayout<>(
//                chs.stream().map(t::getColumnName).collect(toImmutableList()),
//                chs.stream().map(t::getColumnType).collect(toImmutableList()),
//                pk);
//    }
}
