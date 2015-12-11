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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.wrmsr.presto.reactor.tuples.PkLayout;
import com.wrmsr.presto.spi.ConnectorSupport;
import com.wrmsr.presto.spi.ConnectorSupportFactory;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class ConnectorSupportManager
{
    private volatile Set<ConnectorSupportFactory> connectorSupportFactories;

    @Inject
    public ConnectorSupportManager(Set<ConnectorSupportFactory> connectorSupportFactories)
    {
        this.connectorSupportFactories = connectorSupportFactories;
    }

    public Optional<ConnectorSupport> getConnectorSupport(ConnectorSession connectorSession, Connector connector)
    {
        for (ConnectorSupportFactory f : connectorSupportFactories) {
            Optional<ConnectorSupport> cs = f.getConnectorSupport(connectorSession, connector);
            if (cs.isPresent()) {
                return cs;
            }
        }
        return Optional.empty();
    }

    public PkLayout<String> getTableTupleLayout(ConnectorSupport t, SchemaTableName schemaTableName)
    {
        List<String> pk = t.getPrimaryKey(schemaTableName);
        ConnectorSession cs = t.getConnectorSession();
        ConnectorMetadata m = t.getConnector().getMetadata();
        ConnectorTableHandle th = m.getTableHandle(cs, schemaTableName);
        List<ColumnHandle> chs = m.getColumnHandles(cs, th).values().stream().collect(toImmutableList());
        return new PkLayout<>(
                chs.stream().map(t::getColumnName).collect(toImmutableList()),
                chs.stream().map(t::getColumnType).collect(toImmutableList()),
                pk);
    }

}
