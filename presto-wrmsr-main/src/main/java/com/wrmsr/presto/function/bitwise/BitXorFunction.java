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
package com.wrmsr.presto.function.bitwise;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.wrmsr.presto.util.collect.Lists.listOf;

public class BitXorFunction
        extends SqlScalarFunction
{
    public static final BitXorFunction BIT_XOR_FUNCTION = new BitXorFunction();

    public static final String NAME = "bit_xor";
    private static final String FUNCTION_NAME = "bitXor";
    private static final MethodHandle METHOD_HANDLE = methodHandle(BitXorFunction.class, FUNCTION_NAME, long[].class);

    public BitXorFunction()
    {
        super(NAME, ImmutableList.of(), "bigint", ImmutableList.of("bigint"), true);
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.isEmpty());
        return new ScalarFunctionImplementation(false, listOf(arity, false), METHOD_HANDLE.asVarargsCollector(long[].class), true);
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
        return "bitwise xor";
    }

    public static long bitXor(long[] longs)
    {
        checkArgument(longs.length > 0);
        long ret = longs[0];
        for (int i = 0; i < longs.length; ++i) {
            ret ^= longs[i];
        }
        return ret;
    }
}
