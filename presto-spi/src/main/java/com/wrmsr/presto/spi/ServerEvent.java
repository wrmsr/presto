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
package com.wrmsr.presto.spi;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;

import java.util.Map;

public abstract class ServerEvent
{
    public interface Listener
    {
        void onServerEvent(ServerEvent event);
    }

    public static final class MainPluginsLoaded extends ServerEvent
    {
    }

    public static final class MainConnectorsLoaded extends ServerEvent
    {
    }

    public static final class MainDataSourcesLoaded extends ServerEvent
    {
    }

    public static final class PluginLoaded extends ServerEvent
    {
        private final Plugin plugin;

        public PluginLoaded(Plugin plugin)
        {
            this.plugin = plugin;
        }

        public Plugin getPlugin()
        {
            return plugin;
        }
    }

    public static final class ConnectorLoaded extends ServerEvent
    {
        private final String catalogName;
        private final String connectorId;
        private final ConnectorFactory factory;
        private final Map<String, String> properties;

        public ConnectorLoaded(String catalogName, String connectorId, ConnectorFactory factory, Map<String, String> properties)
        {
            this.catalogName = catalogName;
            this.connectorId = connectorId;
            this.factory = factory;
            this.properties = properties;
        }

        public String getCatalogName()
        {
            return catalogName;
        }

        public String getConnectorId()
        {
            return connectorId;
        }

        public ConnectorFactory getFactory()
        {
            return factory;
        }

        public Map<String, String> getProperties()
        {
            return properties;
        }
    }
}
