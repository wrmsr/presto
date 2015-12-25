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

import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.type.ParametricType;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import com.wrmsr.presto.util.Compression;

import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

public class CodecModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeCodecManager.class).asEagerSingleton();

        Multibinder<ParametricType> parametricTypeBinder = Multibinder.newSetBinder(binder, ParametricType.class);
        Multibinder<SqlFunction> functionBinder = Multibinder.newSetBinder(binder, SqlFunction.class);
        Multibinder<TypeCodec> typeCodecBinder = Multibinder.newSetBinder(binder, TypeCodec.class);

        Compression.COMMONS_COMPRESSION_NAMES.stream()
                .map(CommonsCompressionTypeCodec::new)
                .forEach(c -> typeCodecBinder.addBinding().toInstance(c));

        // json, json_values, cbor, cbor_values
        // field strictness, nullability, *_corrupt

        typeCodecBinder.addBinding().toInstance(new JacksonTypeCodec("json", OBJECT_MAPPER.get()));
    }
}
