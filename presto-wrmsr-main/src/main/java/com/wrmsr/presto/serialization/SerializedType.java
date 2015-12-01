package com.wrmsr.presto.serialization;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.util.codec.Codec;
import io.airlift.slice.Slice;

public class SerializedType
    extends AbstractType
{
    public static final String NAME = "serialized";

    private final Type targetType;
    private final Codec<Object, byte[]> codec;

    public SerializedType(Type targetType, String codecName, Codec<Object, byte[]> codec)
    {
        super(new TypeSignature(NAME, ImmutableList.of(targetType.getTypeSignature()), ImmutableList.of(codecName)), Slice.class);
        this.targetType = targetType;
        this.codec = codec;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return null;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return null;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        return null;
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {

    }
}
