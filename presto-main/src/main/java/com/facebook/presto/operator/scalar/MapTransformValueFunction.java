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

package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class MapTransformValueFunction
        extends SqlScalarFunction
{
    public static final MapTransformValueFunction MAP_TRANSFORM_VALUE_FUNCTION = new MapTransformValueFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(
            MapTransformValueFunction.class,
            "transform",
            Type.class,
            Type.class,
            Type.class,
            Block.class,
            MethodHandle.class);

    private MapTransformValueFunction()
    {
        super(new Signature(
                "transform_value",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K"), typeVariable("V1"), typeVariable("V2")),
                ImmutableList.of(),
                parseTypeSignature("map(K,V2)"),
                ImmutableList.of(parseTypeSignature("map(K,V1)"), parseTypeSignature("function(K,V1,V2)")),
                false));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return "apply lambda to each entry of the map and transform the value";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V1");
        Type transformedValueType = boundVariables.getTypeVariable("V2");
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(false, false),
                METHOD_HANDLE.bindTo(keyType).bindTo(valueType).bindTo(transformedValueType),
                isDeterministic());
    }

    public static Block transform(Type keyType, Type valueType, Type transformedValueType, Block block, MethodHandle function)
    {
        int positionCount = block.getPositionCount();
        BlockBuilder resultBuilder = new InterleavedBlockBuilder(ImmutableList.of(keyType, transformedValueType), new BlockBuilderStatus(), positionCount);
        for (int position = 0; position < positionCount; position += 2) {
            Object key = readNativeValue(keyType, block, position);
            Object value = readNativeValue(valueType, block, position + 1);
            Object transformedValue;
            try {
                transformedValue = function.invoke(key, value);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }

            keyType.appendTo(block, position, resultBuilder);
            writeNativeValue(transformedValueType, resultBuilder, transformedValue);
        }
        return resultBuilder.build();
    }
}
