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
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.transaction.LegacyTransactionConnector;
import com.google.inject.Injector;
import com.wrmsr.presto.MainInjector;
import com.wrmsr.presto.spi.ServerEvent;
import com.wrmsr.presto.spi.connectorSupport.ConnectorSupport;
import com.wrmsr.presto.spi.connectorSupport.ConnectorSupportFactory;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConnectorSupportManager
        implements ServerEvent.Listener
{
    private static final Logger log = Logger.get(ConnectorSupportManager.class);

    private final CopyOnWriteArrayList<ConnectorSupportFactory> connectorSupportFactories = new CopyOnWriteArrayList<>();

    private final Optional<Injector> mainInjector;

    @Inject
    public ConnectorSupportManager(
            Set<ConnectorSupportFactory> connectorSupportFactories,
            List<Plugin> mainPlugins,
            @Nullable MainInjector mainInjector)
    {
        this.connectorSupportFactories.addAll(connectorSupportFactories);
        for (Plugin plugin : mainPlugins) {
            register(plugin);
        }
        this.mainInjector = MainInjector.optional(mainInjector);
    }

    @Override
    public synchronized void onServerEvent(ServerEvent event)
    {
        if (event instanceof ServerEvent.PluginLoaded) {
            register(((ServerEvent.PluginLoaded) event).getPlugin());
        }
    }

    private void register(Plugin plugin)
    {
        for (ConnectorSupportFactory connectorSupportFactory : plugin.getServices(ConnectorSupportFactory.class)) {
            register(connectorSupportFactory);
        }
    }

    private void register(ConnectorSupportFactory connectorSupportFactory)
    {
        log.info("Registering connector support factory %s", connectorSupportFactory);
        if (mainInjector.isPresent()) {
            mainInjector.get().injectMembers(connectorSupportFactory);
        }
        connectorSupportFactories.add(connectorSupportFactory);
    }

    // FIXME cache
    public <T extends ConnectorSupport> Optional<T> getConnectorSupport(Class<T> cls, ConnectorSession connectorSession, Connector connector)
    {
        for (ConnectorSupportFactory f : connectorSupportFactories) {
            Optional<T> cs = f.getConnectorSupport(cls, connectorSession, connector);
            if (cs.isPresent()) {
                return cs;
            }
        }

        if (connector instanceof LegacyTransactionConnector) {
            LegacyTransactionConnector wrapperConnector = (LegacyTransactionConnector) connector;
            com.facebook.presto.spi.Connector legacyConnector = wrapperConnector.getConnector();
            for (ConnectorSupportFactory f : connectorSupportFactories) {
                Optional<T> cs = f.getLegacyConnectorSupport(cls, connectorSession, connector, legacyConnector);
                if (cs.isPresent()) {
                    return cs;
                }
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
