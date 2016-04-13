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

import com.facebook.presto.metadata.BoundVariables;
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

public class BitOrFunction
        extends SqlScalarFunction
{
    public static final BitOrFunction BIT_OR_FUNCTION = new BitOrFunction();

    public static final String NAME = "bit_or";
    private static final String FUNCTION_NAME = "bitOr";
    private static final MethodHandle METHOD_HANDLE = methodHandle(BitOrFunction.class, FUNCTION_NAME, long[].class);

    public BitOrFunction()
    {
        super(NAME, ImmutableList.of(), ImmutableList.of(), "bigint", ImmutableList.of("bigint"), true);
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
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
        return "bitwise or";
    }

    public static long bitOr(long[] longs)
    {
        checkArgument(longs.length > 0);
        long ret = longs[0];
        for (int i = 0; i < longs.length; ++i) {
            ret |= longs[i];
        }
        return ret;
    }
}
