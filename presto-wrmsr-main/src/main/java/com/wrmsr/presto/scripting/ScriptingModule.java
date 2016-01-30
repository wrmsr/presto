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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.assistedinject.FactoryProvider;
import com.wrmsr.presto.MainModule;
import com.wrmsr.presto.config.ConfigContainer;
import com.wrmsr.presto.function.FunctionRegistration;
import com.wrmsr.presto.spi.ServerEvent;
import com.wrmsr.presto.spi.scripting.ScriptEngineProvider;

import java.util.List;
import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class ScriptingModule
        extends MainModule
{
    @Override
    public Set<Key> getInjectorForwardings(ConfigContainer config)
    {
        return ImmutableSet.of(
                Key.get(TypeManager.class));
    }

    @Override
    public void configurePlugin(ConfigContainer config, Binder binder)
    {
        binder.bind(ScriptingConfig.class).toInstance(config.getMergedNode(ScriptingConfig.class));

        newSetBinder(binder, ScriptEngineProvider.class);
        for (String name : ImmutableList.of("nashorn")) {
            newSetBinder(binder, ScriptEngineProvider.class).addBinding().toInstance(new BuiltinScriptEngineProvider(name));
        }

        binder.bind(ScriptingManager.class).asEagerSingleton();
        newSetBinder(binder, ServerEvent.Listener.class).addBinding().to(ScriptingManager.class);

        List<Type> SUPPORTED_TYPES = ImmutableList.of(
                BooleanType.BOOLEAN,
                BigintType.BIGINT,
                DoubleType.DOUBLE,
                VarcharType.VARCHAR,
                VarbinaryType.VARBINARY);

        ImmutableList.Builder<ScriptFunction.Config> scriptFunctionConfigs = ImmutableList.builder();
        for (Type type : SUPPORTED_TYPES) {
            String suffix = type.getTypeSignature().getBase().toLowerCase();
            scriptFunctionConfigs.add(new ScriptFunction.Config("eval_" + suffix, type, 0, ScriptFunction.ExecutionType.EVAL));
            for (int arity = 1; arity < 20; ++arity) {
                scriptFunctionConfigs.add(new ScriptFunction.Config("invoke_" + suffix, type, arity, ScriptFunction.ExecutionType.INVOKE));
            }
        }
        scriptFunctionConfigs.add(new ScriptFunction.Config("exec", VarbinaryType.VARBINARY, 0, ScriptFunction.ExecutionType.EVAL));

        binder.bind(ScriptFunction.Factory.class).toProvider(FactoryProvider.newFactory(ScriptFunction.Factory.class, ScriptFunction.class));
        binder.bind(ScriptFunction.Registration.Configs.class).toInstance(new ScriptFunction.Registration.Configs(scriptFunctionConfigs.build()));
        binder.bind(ScriptFunction.Registration.class).asEagerSingleton();
        newSetBinder(binder, FunctionRegistration.class).addBinding().to(ScriptFunction.Registration.class);
    }
}
