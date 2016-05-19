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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.wrmsr.presto.util.collect.Lists.listOf;

// and or xor (scalars + aggs)
// not shl shr lshl lshr (scalars)
public class BitAndFunction
    extends SqlScalarFunction
{
    public static final BitAndFunction BIT_AND_FUNCTION = new BitAndFunction();

    public static final String NAME = "bit_and";
    private static final String FUNCTION_NAME = "bitAnd";
    private static final MethodHandle METHOD_HANDLE = methodHandle(BitAndFunction.class, FUNCTION_NAME, long[].class);

    public BitAndFunction()
    {
        super(new Signature(NAME, SCALAR, ImmutableList.of(), ImmutableList.of(), parseTypeSignature("bigint"), ImmutableList.of(parseTypeSignature("bigint")), true));
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
        return "bitwise and";
    }

    public static long bitAnd(long[] longs)
    {
        checkArgument(longs.length > 0);
        long ret = longs[0];
        for (int i = 0; i < longs.length; ++i) {
            ret &= longs[i];
        }
        return ret;
    }
}
