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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TypeSerde.readType;
import static com.facebook.presto.spi.type.TypeSerde.writeType;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestSerialization
{
    public static class TestingTypeManager
            implements TypeManager
    {
        @Override
        public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
        {
            return false;
        }

        @Override
        public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
        {
            return null;
        }

        @Override
        public Type getType(TypeSignature signature)
        {
            return null;
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return null;
        }

//
//        @Override
//        public Type getType(TypeSignature signature)
//        {
//            for (Type type : getTypes()) {
//                if (signature.equals(type.getTypeSignature())) {
//                    return type;
//                }
//            }
//            return null;
//        }
//
//        @Override
//        public Type getParameterizedType(String baseTypeName, List<TypeSignature> typeParameters, List<Object> literalParameters)
//        {
//            return getType(new TypeSignature(baseTypeName, typeParameters, literalParameters));
//        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.<Type>of(BOOLEAN, BIGINT, DOUBLE, VARCHAR, VARBINARY, TIMESTAMP, DATE, HYPER_LOG_LOG);
        }

        @Override
        public Optional<Type> getCommonSuperType(List<? extends Type> types)
        {
            return Optional.empty();
        }

        @Override
        public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
        {
            return Optional.empty();
        }
    }

    @Test
    public void testShit() throws Throwable
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writeType(sliceOutput, BOOLEAN);
        Type actualType = readType(new TestingTypeManager(), sliceOutput.slice().getInput());
        assertEquals(actualType, BOOLEAN);

        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(BIGINT, VARCHAR), new BlockBuilderStatus(), 10);
        BIGINT.writeLong(blockBuilder, 12);
        VARCHAR.writeSlice(blockBuilder, Slices.allocate(7));
    }
}
