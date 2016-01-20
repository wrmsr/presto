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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.wrmsr.presto.util.codec.Codec;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class EncodeFunction
        extends SqlScalarFunction
{
    private final TypeCodec typeCodec;
    private static final MethodHandle METHOD_HANDLE = methodHandle(EncodeFunction.class, "encodeSlice", Codec.class, Slice.class);

    public EncodeFunction(TypeCodec typeCodec)
    {
        super(typeCodec.getName(), ImmutableList.of(typeParameter("T")), typeCodec.getName() + "<T>", ImmutableList.of("T"));
        this.typeCodec = typeCodec;
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1);
        Type fromType = Iterables.getOnlyElement(types.values());
        Codec<Slice, Slice> codec = typeCodec.getSliceCodec(fromType);
        MethodHandle mh = METHOD_HANDLE.bindTo(codec);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), mh, true);
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "encode " + typeCodec.getName();
    }

    public static Slice encodeSlice(Codec<Slice, Slice> codec, Slice slice)
    {
        return codec.encode(slice);
    }
}
