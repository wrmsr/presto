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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.block.BlockUtil.checkValidPositions;
import static com.facebook.presto.spi.block.DictionaryId.randomDictionaryId;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.Slices.copyOf;
import static io.airlift.slice.Slices.wrappedIntArray;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class DictionaryBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DictionaryBlock.class).instanceSize();

    private final int positionCount;
    private final Block dictionary;
    private final Slice ids;
    private final int retainedSizeInBytes;
    private final int sizeInBytes;
    private final int uniqueIds;
    private final DictionaryId dictionarySourceId;

    public DictionaryBlock(int positionCount, Block dictionary, Slice ids)
    {
        this(positionCount, dictionary, ids, false, randomDictionaryId());
    }

    public DictionaryBlock(int positionCount, Block dictionary, Slice ids, DictionaryId dictionaryId)
    {
        this(positionCount, dictionary, ids, false, dictionaryId);
    }

    public DictionaryBlock(int positionCount, Block dictionary, Slice ids, boolean dictionaryIsCompacted)
    {
        this(positionCount, dictionary, ids, dictionaryIsCompacted, randomDictionaryId());
    }

    public DictionaryBlock(int positionCount, Block dictionary, Slice ids, boolean dictionaryIsCompacted, DictionaryId dictionarySourceId)
    {
        requireNonNull(dictionary, "dictionary is null");
        requireNonNull(ids, "ids is null");

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        if (ids.length() != positionCount * SIZE_OF_INT) {
            throw new IllegalArgumentException("ids length does not match with positionCount");
        }

        this.positionCount = positionCount;
        this.dictionary = dictionary;
        this.ids = ids;
        this.dictionarySourceId = requireNonNull(dictionarySourceId, "dictionarySourceId is null");
        this.retainedSizeInBytes = INSTANCE_SIZE + dictionary.getRetainedSizeInBytes() + ids.getRetainedSize();

        if (dictionaryIsCompacted) {
            this.sizeInBytes = this.retainedSizeInBytes;
            this.uniqueIds = dictionary.getPositionCount();
        }
        else {
            int sizeInBytes = 0;
            int uniqueIds = 0;
            boolean[] seen = new boolean[dictionary.getPositionCount()];
            for (int i = 0; i < positionCount; i++) {
                int position = getIndex(ids, i);
                if (!seen[position]) {
                    if (!dictionary.isNull(position)) {
                        sizeInBytes += dictionary.getLength(position);
                    }
                    uniqueIds++;
                    seen[position] = true;
                }
            }
            this.sizeInBytes = sizeInBytes + ids.length();
            this.uniqueIds = uniqueIds;
        }
    }

    @Override
    public int getLength(int position)
    {
        return dictionary.getLength(getIndex(position));
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return dictionary.getByte(getIndex(position), offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return dictionary.getShort(getIndex(position), offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return dictionary.getInt(getIndex(position), offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return dictionary.getLong(getIndex(position), offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return dictionary.getSlice(getIndex(position), offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        return dictionary.getObject(getIndex(position), clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return dictionary.bytesEqual(getIndex(position), offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return dictionary.bytesCompare(getIndex(position), offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        dictionary.writeBytesTo(getIndex(position), offset, length, blockBuilder);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        dictionary.writePositionTo(getIndex(position), blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return dictionary.equals(getIndex(position), offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        return dictionary.hash(getIndex(position), offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return dictionary.compareTo(getIndex(leftPosition), leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return dictionary.getSingleValueBlock(getIndex(position));
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new DictionaryBlockEncoding(dictionary.getEncoding());
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        checkValidPositions(positions, positionCount);

        List<Integer> positionsToCopy = new ArrayList<>();
        Map<Integer, Integer> oldIndexToNewIndex = new HashMap<>();
        int[] newIds = new int[positions.size()];

        for (int i = 0; i < positions.size(); i++) {
            int oldIndex = getIndex(positions.get(i));
            if (!oldIndexToNewIndex.containsKey(oldIndex)) {
                oldIndexToNewIndex.put(oldIndex, positionsToCopy.size());
                positionsToCopy.add(oldIndex);
            }
            newIds[i] = oldIndexToNewIndex.get(oldIndex);
        }
        return new DictionaryBlock(positions.size(), dictionary.copyPositions(positionsToCopy), wrappedIntArray(newIds));
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }
        Slice newIds = ids.slice(positionOffset * SIZE_OF_INT, length * SIZE_OF_INT);
        return new DictionaryBlock(length, dictionary, newIds);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        if (position < 0 || length < 0 || position + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + position + " in block with " + positionCount + " positions");
        }
        Slice newIds = copyOf(ids, position * SIZE_OF_INT, length * SIZE_OF_INT);
        DictionaryBlock dictionaryBlock = new DictionaryBlock(length, dictionary, newIds);
        return dictionaryBlock.compact();
    }

    @Override
    public boolean isNull(int position)
    {
        return dictionary.isNull(getIndex(position));
    }

    public Block getDictionary()
    {
        return dictionary;
    }

    public Slice getIds()
    {
        return ids;
    }

    public int getId(int position)
    {
        return ids.getInt(position * SIZE_OF_INT);
    }

    public DictionaryId getDictionarySourceId()
    {
        return dictionarySourceId;
    }

    public boolean isCompact()
    {
        return uniqueIds == dictionary.getPositionCount();
    }

    private int getIndex(int position)
    {
        return getIndex(ids, position);
    }

    public DictionaryBlock compact()
    {
        if (isCompact()) {
            return this;
        }

        // determine which dictionary entries are referenced and build a reindex for them
        int dictionarySize = dictionary.getPositionCount();
        List<Integer> dictionaryPositionsToCopy = new ArrayList<>(min(dictionarySize, positionCount));
        int[] remapIndex = new int[dictionarySize];
        Arrays.fill(remapIndex, -1);

        int newIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int dictionaryIndex = getIndex(i);
            if (remapIndex[dictionaryIndex] == -1) {
                dictionaryPositionsToCopy.add(dictionaryIndex);
                remapIndex[dictionaryIndex] = newIndex;
                newIndex++;
            }
        }

        // entire dictionary is referenced
        if (dictionaryPositionsToCopy.size() == dictionarySize) {
            return this;
        }

        // compact the dictionary
        int[] newIds = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            int newId = remapIndex[getIndex(i)];
            if (newId == -1) {
                throw new IllegalStateException("reference to a non-existent key");
            }
            newIds[i] = newId;
        }
        try {
            Block compactDictionary = dictionary.copyPositions(dictionaryPositionsToCopy);
            return new DictionaryBlock(positionCount, compactDictionary, wrappedIntArray(newIds), true);
        }
        catch (UnsupportedOperationException e) {
            // ignore if copy positions is not supported for the dictionary block
            return this;
        }
    }

    private static int getIndex(Slice ids, int i)
    {
        return ids.getInt(i * SIZE_OF_INT);
    }
}
