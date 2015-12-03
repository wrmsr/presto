package com.wrmsr.presto.codec;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
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
        public Type getType(TypeSignature signature)
        {
            for (Type type : getTypes()) {
                if (signature.equals(type.getTypeSignature())) {
                    return type;
                }
            }
            return null;
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignature> typeParameters, List<Object> literalParameters)
        {
            return getType(new TypeSignature(baseTypeName, typeParameters, literalParameters));
        }

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
