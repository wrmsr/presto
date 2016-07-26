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
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class TypeCodecManager
{
    private final Set<TypeCodec> initialTypeCodecs;
    private final Metadata metadata;
    private final TypeRegistry typeRegistry;

    private final Map<String, TypeCodec> typeCodecs = new MapMaker().makeMap();

    @Inject
    public TypeCodecManager(Set<TypeCodec> initialTypeCodecs, Metadata metadata, TypeRegistry typeRegistry)
    {
        this.initialTypeCodecs = initialTypeCodecs;
        this.metadata = metadata;
        this.typeRegistry = typeRegistry;
    }

    @PostConstruct
    private void registerInitialTypeCodecs()
    {
        for (TypeCodec tc : initialTypeCodecs) {
            addTypeCodec(tc);
        }
    }

    public synchronized void addTypeCodec(TypeCodec typeCodec)
    {
        checkArgument(!typeCodecs.containsKey(typeCodec.getName()));
        ParametricType pt = new EncodedParametricType(typeCodec);
        typeRegistry.addParametricType(pt);
        SqlFunction ef = new EncodeFunction(typeCodec);
        SqlFunction df = new DecodeFunction(typeCodec);
        SqlFunction cf0 = new VarbinaryToEncodedCast(typeCodec);
        SqlFunction cf1 = new EncodedToVarbinaryCast(typeCodec);
        metadata.addFunctions(ImmutableList.of(ef, df, cf0, cf1));
        typeCodecs.put(typeCodec.getName(), typeCodec);
    }
}
