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
package com.wrmsr.presto.codec;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.wrmsr.presto.MainModule;
import com.wrmsr.presto.config.ConfigContainer;
import com.wrmsr.presto.util.Compression;

import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class CodecModule
        extends MainModule
{
    @Override
    public Set<Key> getInjectorForwardings(ConfigContainer config)
    {
        return ImmutableSet.of(
                Key.get(TypeManager.class),
                Key.get(Metadata.class));
    }

    @Override
    public void configurePlugin(ConfigContainer config, Binder binder)
    {
        binder.bind(TypeCodecManager.class).asEagerSingleton();

        newSetBinder(binder, ParametricType.class);
        newSetBinder(binder, SqlFunction.class);
        newSetBinder(binder, TypeCodec.class);

        Compression.COMMONS_COMPRESSION_NAMES.stream()
                .map(CommonsCompressionTypeCodec::new)
                .forEach(c -> newSetBinder(binder, TypeCodec.class).addBinding().toInstance(c));

        // json, json_values, cbor, cbor_values
        // field strictness, nullability, *_corrupt

        // newSetBinder(binder, TypeCodec.class).addBinding().toInstance(new JacksonTypeCodec("json", OBJECT_MAPPER.get()));
    }
}
