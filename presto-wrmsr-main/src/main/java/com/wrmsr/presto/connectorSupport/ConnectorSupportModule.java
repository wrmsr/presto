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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import com.wrmsr.presto.MainModule;
import com.wrmsr.presto.config.ConfigContainer;
import com.wrmsr.presto.connector.jdbc.ExtendedJdbcConnector;
import com.wrmsr.presto.spi.ServerEvent;
import com.wrmsr.presto.spi.connectorSupport.ConnectorSupportFactory;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class ConnectorSupportModule
    extends MainModule
{
    @Override
    public void configurePlugin(ConfigContainer config, Binder binder)
    {
        binder.bind(ConnectorSupportManager.class).asEagerSingleton();
        newSetBinder(binder, ServerEvent.Listener.class).addBinding().to(ConnectorSupportManager.class);

        newSetBinder(binder, ConnectorSupportFactory.class).addBinding().toInstance(new ConnectorSupportFactory.Default(
                ExtendedJdbcConnectorSupport.class, ExtendedJdbcConnector.class, ExtendedJdbcConnectorSupport::new));
    }
}
