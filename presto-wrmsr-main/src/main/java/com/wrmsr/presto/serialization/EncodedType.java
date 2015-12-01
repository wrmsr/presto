package com.wrmsr.presto.serialization;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

public class EncodedType
        extends AbstractType
{
    public static final String NAME = "encoded";

    private final TypeCodec typeCodec;

    public EncodedType(TypeCodec typeCodec)
    {
        super(new TypeSignature(NAME, ImmutableList.of(typeCodec.getFromType().getTypeSignature()), ImmutableList.of(typeCodec.getName())),
                typeCodec.getToType().getJavaType());
        this.typeCodec = typeCodec;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return typeCodec.getToType().createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return typeCodec.getToType().createBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        return typeCodec.getCodec().decode(typeCodec.getFromType().getObjectValue(session, block, position));
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        // typeCodec.getToType().appendTo(block,)
    }

    @Override
    public Block getObject(Block block, int position)
    {
        return block.getObject(position, Block.class);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        blockBuilder.writeObject(value).closeEntry();
    }
}
