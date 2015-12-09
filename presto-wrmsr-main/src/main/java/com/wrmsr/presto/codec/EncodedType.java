package com.wrmsr.presto.codec;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;

public class EncodedType
        extends AbstractVariableWidthType
{
    private final TypeCodec typeCodec;
    private final Type fromType;

    // FIXME FUCK install serdes
    @JsonCreator
    public EncodedType(TypeCodec typeCodec, Type fromType)
    {
        super(parameterizedTypeName(typeCodec.getName(), fromType.getTypeSignature()), Slice.class);
        this.typeCodec = typeCodec;
        this.fromType = fromType;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return new SqlVarbinary(block.getSlice(position, 0, block.getLength(position)).getBytes());
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int leftLength = leftBlock.getLength(leftPosition);
        int rightLength = rightBlock.getLength(rightPosition);
        if (leftLength != rightLength) {
            return false;
        }
        return leftBlock.equals(leftPosition, 0, rightBlock, rightPosition, 0, leftLength);
    }

    @Override
    public int hash(Block block, int position)
    {
        return block.hash(position, 0, block.getLength(position));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int leftLength = leftBlock.getLength(leftPosition);
        int rightLength = rightBlock.getLength(rightPosition);
        return leftBlock.compareTo(leftPosition, 0, leftLength, rightBlock, rightPosition, 0, rightLength);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, block.getLength(position), blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getLength(position));
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
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
