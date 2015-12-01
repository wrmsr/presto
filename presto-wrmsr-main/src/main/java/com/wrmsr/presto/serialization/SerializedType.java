package com.wrmsr.presto.serialization;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.TypeSignature;

public class SerializedType
    extends AbstractType
{
    public static final String NAME = "serialized";

    public SerializedType(Type targetType, String codecName, )
    {
        super(signature, javaType);
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
