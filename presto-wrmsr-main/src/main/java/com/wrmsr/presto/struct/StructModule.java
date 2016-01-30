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
package com.wrmsr.presto.struct;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.wrmsr.presto.MainModule;
import com.wrmsr.presto.codec.TypeCodec;
import com.wrmsr.presto.config.ConfigContainer;

import java.util.List;
import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class StructModule
        extends MainModule
{
    @Override
    public Set<Key> getInjectorForwardings(ConfigContainer config)
    {
        return ImmutableSet.of(
                Key.get(SqlParser.class),
                Key.get(new TypeLiteral<List<PlanOptimizer>>() {}),
                Key.get(FeaturesConfig.class),
                Key.get(AccessControl.class),
                Key.get(BlockEncodingSerde.class),
                Key.get(SessionPropertyManager.class),
                Key.get(ConnectorManager.class),
                Key.get(TypeRegistry.class),
                Key.get(TypeManager.class),
                Key.get(Metadata.class));
    }


    @Override
    public void configurePlugin(ConfigContainer config, Binder binder)
    {
        binder.bind(StructManager.class).asEagerSingleton();

        Multibinder<SqlFunction> functionBinder = newSetBinder(binder, SqlFunction.class);

        binder.bind(DefineStructForQueryFunction.class).asEagerSingleton();
        functionBinder.addBinding().to(DefineStructForQueryFunction.class);

        binder.bind(DefineStructFunction.class).asEagerSingleton();
        functionBinder.addBinding().to(DefineStructFunction.class);

        newSetBinder(binder, TypeCodec.class).addBinding().toInstance(new FlatTypeCodec());
    }
}
