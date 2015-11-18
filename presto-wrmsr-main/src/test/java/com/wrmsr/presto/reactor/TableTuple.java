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
package com.wrmsr.presto.reactor;

import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Objects;

public class TableTuple
{
    protected final TableTupleLayout layout;
    protected final List<Object> values;

    public TableTuple(TableTupleLayout layout, List<Object> values)
    {
        this.layout = layout;
        this.values = ImmutableList.copyOf(values);
    }

    public TableTupleLayout getLayout()
    {
        return layout;
    }

    public List<Object> getValues()
    {
        return values;
    }

    @Override
    public String toString()
    {
        return "Tuple{" +
                "layout=" + layout +
                ", values=" + values +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableTuple tuple = (TableTuple) o;
        return Objects.equals(layout, tuple.layout) &&
                Objects.equals(values, tuple.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(layout, values);
    }

    public RecordSet getRecordSet()
    {
        return new InMemoryRecordSet(layout.getTypes(), ImmutableList.of(values));
    }

    public RecordCursor getRecordCursor()
    {
        return getRecordSet().cursor();
    }

    public static Block toBlock(TableTupleLayout layout, List<Object> values)
    {
        BlockBuilder b = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 10000);
        for (int i = 0; i < values.size(); ++i) {
            b.write(layout.getTypes().get(i), values.get(i));
        }
        return b.build();
    }

    public static Slice toSlice(TableTupleLayout layout, List<Object> values, BlockEncodingSerde blockEncodingSerde)
    {
        Block block = toBlock(layout, values);
        SliceOutput output = new DynamicSliceOutput(64);
        BlockEncoding encoding = block.getEncoding();
        blockEncodingSerde.writeBlockEncoding(output, encoding);
        encoding.writeBlock(output, block);
        return output.slice();
    }

    public Block toBlock()
    {
        return toBlock(layout, values);
    }

    public Slice toSlice(BlockEncodingSerde blockEncodingSerde)
    {
        return toSlice(layout, values, blockEncodingSerde);
    }

    // TODO: terse jackson serializer too for pk

    public byte[] toBytes(BlockEncodingSerde blockEncodingSerde)
    {
        return toSlice(blockEncodingSerde).getBytes();
    }

    public static TableTuple fromBlock(TableTupleLayout layout, Block block)
    {
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        for (int i = 0; i < layout.getTypes().size(); ++i) {
            builder.add(block.read(layout.getTypes().get(i), i));
        }
        return new TableTuple(layout, builder.build());
    }

    public static TableTuple fromSlice(TableTupleLayout layout, Slice slice, BlockEncodingSerde blockEncodingSerde)
    {
        BasicSliceInput input = slice.getInput();
        BlockEncoding blockEncoding = blockEncodingSerde.readBlockEncoding(input);
        return fromBlock(layout, blockEncoding.readBlock(input));
    }

    public static TableTuple fromBytes(TableTupleLayout layout, byte[] bytes, BlockEncodingSerde blockEncodingSerde)
    {
        return fromSlice(layout, Slices.wrappedBuffer(bytes), blockEncodingSerde);
    }
}
