package com.wrmsr.presto.codec;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

public class SerializedType
    extends AbstractType
{
    public static final String NAME = "serialized";

    private final Type targetType;
    private final Serializer serializer;

    public SerializedType(Type targetType, Serializer serializer)
    {
        super(new TypeSignature(NAME, ImmutableList.of(targetType.getTypeSignature()), ImmutableList.of(serializer.getName())), Slice.class);
        this.targetType = targetType;
        this.serializer = serializer;
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
