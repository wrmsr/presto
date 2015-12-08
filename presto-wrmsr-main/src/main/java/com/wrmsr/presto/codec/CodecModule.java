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

import com.facebook.presto.metadata.FunctionResolver;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import com.wrmsr.presto.function.FunctionRegistration;
import com.wrmsr.presto.type.ParametricTypeRegistration;
import com.wrmsr.presto.util.Compression;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class CodecModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeCodecManager.class).asEagerSingleton();
        binder.bind(TypeCodecProvider.class).to(TypeCodecManager.class);

        Multibinder<TypeCodecProvider> typeCodecProviderBinder = Multibinder.newSetBinder(binder, TypeCodecProvider.class);
        Multibinder<FunctionResolver> functionResolverBinder = Multibinder.newSetBinder(binder, FunctionResolver.class);
        Multibinder<ParametricTypeRegistration> parametricTypeRegistrationBinder = Multibinder.newSetBinder(binder, ParametricTypeRegistration.class);

        binder.bind(EncodedParametricType.class).asEagerSingleton();
        parametricTypeRegistrationBinder.addBinding().to(EncodedParametricType.class);

        typeCodecProviderBinder.addBinding().toInstance(
                TypeCodecProvider.of(
                        Compression.COMMONS_COMPRESSION_NAMES.stream()
                                .map(CommonsCompressionTypeCodec::new)
                                .collect(toImmutableList())));

        Multibinder<FunctionRegistration> functionRegistrationBinder = Multibinder.newSetBinder(binder, FunctionRegistration.class);

        binder.bind(EncodeFunction.class).asEagerSingleton();
        functionRegistrationBinder.addBinding().to(EncodeFunction.class);
        functionResolverBinder.addBinding().to(EncodeFunction.class);
        binder.bind(DecodeFunction.class).asEagerSingleton();
        functionRegistrationBinder.addBinding().to(DecodeFunction.class);
    }
}
