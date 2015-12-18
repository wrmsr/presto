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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.wrmsr.presto.codec.CodecModule;
import com.wrmsr.presto.config.ConfigModule;
import com.wrmsr.presto.config.ConfigContainer;
import com.wrmsr.presto.connector.ConnectorModule;
import com.wrmsr.presto.connectorSupport.ConnectorSupportModule;
import com.wrmsr.presto.function.FunctionModule;
import com.wrmsr.presto.scripting.ScriptingModule;
import com.wrmsr.presto.serialization.SerializationModule;
import com.wrmsr.presto.struct.StructModule;
import com.wrmsr.presto.type.TypeModule;

public class MainPluginModule
        implements Module
{
    private final ConfigContainer config;

    public MainPluginModule(ConfigContainer config)
    {
        this.config = config;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.install(new ConfigModule(config));

        binder.install(new CodecModule());
        binder.install(new ConnectorModule());
        binder.install(new ConnectorSupportModule());
        binder.install(new FunctionModule());
        binder.install(new ScriptingModule(config));
        binder.install(new SerializationModule());
        binder.install(new StructModule());
        binder.install(new TypeModule());
    }
}