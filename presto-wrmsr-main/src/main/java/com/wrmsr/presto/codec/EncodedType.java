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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.type.WrapperType;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static java.util.Objects.requireNonNull;

public class EncodedType
        extends AbstractType
        implements WrapperType
{
    private final TypeCodec typeCodec;
    private final Type fromType;
    private final TypeCodec.Specialization specialization;
    private final Type underlyingType;

    // FIXME FUCK install serdes olol
    @JsonCreator
    public EncodedType(TypeCodec typeCodec, TypeCodec.Specialization specialization, Type fromType)
    {
        super(parameterizedTypeName(typeCodec.getName(), fromType.getTypeSignature()), typeCodec.getJavaType());
        this.typeCodec = requireNonNull(typeCodec);
        this.specialization = requireNonNull(specialization);
        this.fromType = requireNonNull(fromType);
        this.underlyingType = specialization.getUnderlyingType();
    }

    @Override
    public Type getWrappedType()
    {
        return fromType;
    }

    @Override
    public boolean isAnnotaitonType()
    {
        return specialization.isAnnotation();
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return ImmutableList.of(fromType);
    }

    @Override
    public String getDisplayName()
    {
        return typeCodec.getName() + "<" + fromType.getDisplayName() + ">";
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        underlyingType.appendTo(block, position, blockBuilder);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return underlyingType.compareTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return underlyingType.createBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return underlyingType.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return underlyingType.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        return underlyingType.getBoolean(block, position);
    }

    @Override
    public double getDouble(Block block, int position)
    {
        return underlyingType.getDouble(block, position);
    }

    @Override
    public long getLong(Block block, int position)
    {
        return underlyingType.getLong(block, position);
    }

    @Override
    public Object getObject(Block block, int position)
    {
        return underlyingType.getObject(block, position);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        return underlyingType.getObjectValue(session, block, position);
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return underlyingType.getSlice(block, position);
    }

    @Override
    public int hash(Block block, int position)
    {
        return underlyingType.hash(block, position);
    }

    @Override
    public boolean isComparable()
    {
        return underlyingType.isComparable();
    }

    @Override
    public boolean isOrderable()
    {
        return underlyingType.isOrderable();
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        underlyingType.writeBoolean(blockBuilder, value);
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        underlyingType.writeDouble(blockBuilder, value);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        underlyingType.writeLong(blockBuilder, value);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        underlyingType.writeObject(blockBuilder, value);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        underlyingType.writeSlice(blockBuilder, value);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        underlyingType.writeSlice(blockBuilder, value, offset, length);
    }
}
