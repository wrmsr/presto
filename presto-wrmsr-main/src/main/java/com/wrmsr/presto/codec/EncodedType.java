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
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static java.util.Objects.requireNonNull;

public class EncodedType
        extends AbstractType
{
    private final TypeCodec typeCodec;
    private final Type fromType;
    private final TypeCodec.Specialization specialization;
    private final Type underlyingType;

    // FIXME FUCK install serdes
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
        fromType.appendTo(block, position, blockBuilder);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return fromType.compareTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return fromType.createBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return fromType.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return fromType.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        return fromType.getBoolean(block, position);
    }

    @Override
    public double getDouble(Block block, int position)
    {
        return fromType.getDouble(block, position);
    }

    @Override
    public long getLong(Block block, int position)
    {
        return fromType.getLong(block, position);
    }

    @Override
    public Object getObject(Block block, int position)
    {
        return fromType.getObject(block, position);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        return fromType.getObjectValue(session, block, position);
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return fromType.getSlice(block, position);
    }

    @Override
    public int hash(Block block, int position)
    {
        return fromType.hash(block, position);
    }

    @Override
    public boolean isComparable()
    {
        return fromType.isComparable();
    }

    @Override
    public boolean isOrderable()
    {
        return fromType.isOrderable();
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        fromType.writeBoolean(blockBuilder, value);
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        fromType.writeDouble(blockBuilder, value);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        fromType.writeLong(blockBuilder, value);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        fromType.writeObject(blockBuilder, value);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        fromType.writeSlice(blockBuilder, value);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        fromType.writeSlice(blockBuilder, value, offset, length);
    }
}
