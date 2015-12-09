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

import java.util.List;

import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;

public class EncodedType
        extends AbstractType
{
    private final TypeCodec typeCodec;
    private final Type fromType;

    public EncodedType(TypeCodec typeCodec, Type fromType)
    {
        super(parameterizedTypeName(typeCodec.getName(), fromType.getTypeSignature()), Slice.class);
        this.typeCodec = typeCodec;
        this.fromType = fromType;
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

    @Override
    public List<Type> getTypeParameters()
    {
        return ImmutableList.of(fromType);
    }

    @Override
    public String getDisplayName()
    {
        return "array<" + fromType.getDisplayName() + ">";
    }
}
