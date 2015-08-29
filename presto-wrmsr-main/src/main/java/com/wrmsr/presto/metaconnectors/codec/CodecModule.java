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
package com.wrmsr.presto.metaconnectors.codec;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.wrmsr.presto.metaconnectors.codec.csv.CsvDecoderModule;
import com.wrmsr.presto.metaconnectors.codec.dummy.DummyDecoderModule;
import com.wrmsr.presto.metaconnectors.codec.json.JsonDecoderModule;
import com.wrmsr.presto.metaconnectors.codec.raw.RawDecoderModule;

/**
 * decoder specific module. Installs the registry and all known decoder submodules.
 */
public class CodecModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(CodecRegistry.class).in(Scopes.SINGLETON);

        binder.install(new DummyDecoderModule());
        binder.install(new CsvDecoderModule());
        binder.install(new JsonDecoderModule());
        binder.install(new RawDecoderModule());
    }

    public static void bindRowDecoder(Binder binder, Class<? extends RowCodec> decoderClass)
    {
        Multibinder<RowCodec> rowDecoderBinder = Multibinder.newSetBinder(binder, RowCodec.class);
        rowDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }

    public static void bindFieldDecoder(Binder binder, Class<? extends FieldCodec<?>> decoderClass)
    {
        Multibinder<FieldCodec<?>> fieldDecoderBinder = Multibinder.newSetBinder(binder, new TypeLiteral<FieldCodec<?>>() {});
        fieldDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }
}
