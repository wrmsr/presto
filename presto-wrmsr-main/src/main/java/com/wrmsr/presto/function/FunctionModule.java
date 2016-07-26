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
package com.wrmsr.presto.function;

import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.wrmsr.presto.MainModule;
import com.wrmsr.presto.config.ConfigContainer;
import com.wrmsr.presto.function.bitwise.BitwiseModule;

import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class FunctionModule
        extends MainModule.Composite
{
    public FunctionModule()
    {
        super(new BitwiseModule());
    }

    @Override
    protected Set<Key> getInjectorForwardingsParent(ConfigContainer config)
    {
        return ImmutableSet.of(
                Key.get(TypeManager.class));
    }

    @Override
    protected void configurePluginParent(ConfigContainer config, Binder binder)
    {
        newSetBinder(binder, SqlFunction.class);
        newSetBinder(binder, FunctionRegistration.class);

        newSetBinder(binder, FunctionRegistration.class).addBinding().toInstance(FunctionRegistration.of(flb -> {
            flb
                    .scalars(CompressionFunctions.class)
                    .scalars(GrokFunctions.class)
                    .scalars(FileFunctions.class);
        }));
    }
}
