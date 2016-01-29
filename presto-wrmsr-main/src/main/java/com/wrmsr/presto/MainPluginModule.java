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
package com.wrmsr.presto;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.wrmsr.presto.codec.CodecModule;
import com.wrmsr.presto.config.ConfigContainer;
import com.wrmsr.presto.config.ConfigModule;
import com.wrmsr.presto.connector.ConnectorModule;
import com.wrmsr.presto.connectorSupport.ConnectorSupportModule;
import com.wrmsr.presto.eval.EvalModule;
import com.wrmsr.presto.function.FunctionModule;
import com.wrmsr.presto.scripting.ScriptingModule;
import com.wrmsr.presto.serialization.SerializationModule;
import com.wrmsr.presto.spi.ServerEvent;
import com.wrmsr.presto.struct.StructModule;
import com.wrmsr.presto.type.TypeModule;
import com.wrmsr.presto.util.GuiceUtils;

import java.util.List;
import java.util.Map;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class MainPluginModule
        extends MainModule.Composite
{
    public MainPluginModule()
    {
        super(
                new ConfigModule(),
                new CodecModule(),
                new ConnectorModule(),
                new ConnectorSupportModule(),
                new EvalModule(),
                new FunctionModule(),
                new ScriptingModule(),
                new SerializationModule(),
                new StructModule(),
                new TypeModule());
    }

    @Override
    protected void configurePluginParent(ConfigContainer config, Binder binder)
    {
        newSetBinder(binder, ServerEvent.Listener.class);
        binder.install(new GuiceUtils.EmptyModule()
        {
            @Provides
            @Singleton
            public List<Plugin> provideMainPlugins(PluginManager pluginManager)
            {
                return pluginManager.getLoadedPlugins();
            }

            @Provides
            @Singleton
            public Map<String, Connector> provideMainConnectors(ConnectorManager connectorManager)
            {
                return connectorManager.getConnectors();
            }
        });
    }
}
