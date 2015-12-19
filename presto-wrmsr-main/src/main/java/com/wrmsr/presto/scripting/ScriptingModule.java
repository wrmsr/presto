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
package com.wrmsr.presto.scripting;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.assistedinject.FactoryProvider;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.wrmsr.presto.config.ConfigContainer;
import com.wrmsr.presto.function.FunctionRegistration;
import com.wrmsr.presto.spi.scripting.ScriptEngineProvider;

public class ScriptingModule
        implements Module
{
    private final ConfigContainer config;

    public ScriptingModule(ConfigContainer config)
    {
        this.config = config;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(ScriptingConfig.class).toInstance(config.getMergedNode(ScriptingConfig.class));

        Multibinder<FunctionRegistration> functionRegistrationBinder = Multibinder.newSetBinder(binder, FunctionRegistration.class);
        MapBinder<String, ScriptEngineProvider> scriptEngineProviderBinder = MapBinder.newMapBinder(binder, String.class, ScriptEngineProvider.class);

        for (String name : ImmutableList.of("nashorn")) {
            scriptEngineProviderBinder.addBinding(name).toInstance(new BuiltinScriptEngineProvider(name));
        }

        binder.bind(ScriptingManager.class).asEagerSingleton();

        binder.bind(ScriptFunction.Factory.class).toProvider(FactoryProvider.newFactory(ScriptFunction.Factory.class, ScriptFunction.class));
        binder.bind(ScriptFunction.Registration.MaxArity.class).toInstance(new ScriptFunction.Registration.MaxArity(3));
        binder.bind(ScriptFunction.Registration.class).asEagerSingleton();
        functionRegistrationBinder.addBinding().to(ScriptFunction.Registration.class);
    }
}
