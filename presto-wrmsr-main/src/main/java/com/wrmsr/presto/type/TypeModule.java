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
package com.wrmsr.presto.type;

import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.multibindings.Multibinder;
import com.wrmsr.presto.MainModule;
import com.wrmsr.presto.config.ConfigContainer;
import com.wrmsr.presto.function.FunctionRegistration;

import java.util.Set;

public class TypeModule
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
        Multibinder<Type> typeBinder = Multibinder.newSetBinder(binder, Type.class);
        Multibinder<ParametricType> parametricTypeBinder = Multibinder.newSetBinder(binder, ParametricType.class);

        typeBinder.addBinding().toInstance(PropertiesType.PROPERTIES);

        Multibinder<FunctionRegistration> functionRegistrationBinder = Multibinder.newSetBinder(binder, FunctionRegistration.class);

        binder.bind(PropertiesFunction.class).asEagerSingleton();
        functionRegistrationBinder.addBinding().to(PropertiesFunction.class);
    }
}
