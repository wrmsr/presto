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
package com.wrmsr.presto;

import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Optional;

public class TestFileBuffers
{
    public static class RandomAccessSliceFileReader
            implements AutoCloseable
    {
        protected final File file;
        protected final String mode;
        protected final FileChannel.MapMode mapMode;
        protected final long mapPosition;

        protected long mapLength;

        protected RandomAccessFile randomAccessFile;
        protected FileChannel fileChannel;
        protected MappedByteBuffer mappedByteBuffer;

        public RandomAccessSliceFileReader(File file)
        {
            this(file, "r", FileChannel.MapMode.READ_ONLY, 0L, Optional.empty());
        }

        protected RandomAccessSliceFileReader(File file, String mode, FileChannel.MapMode mapMode, long mapPosition, Optional<Long> initialLength)
        {
            this.file = file;
            this.mode = mode;
            this.mapMode = mapMode;
            this.mapPosition = mapPosition;
            open();
            map(initialLength.isPresent() ? initialLength.get() : file.length());
        }

        protected void open()
        {
            try {
                randomAccessFile = new RandomAccessFile(file, mode);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        protected void map(long length)
        {
            unmap();
            try {
                fileChannel = randomAccessFile.getChannel();
                mappedByteBuffer = fileChannel.map(mapMode, mapPosition, length);
                // mappedByteBuffer.load();
                this.mapLength = length;
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        public static void unmapBuffer(Buffer buffer)
        {
            sun.misc.Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
            if (cleaner != null) {
                cleaner.clean();
            }
        }

        protected void unmap()
        {
            if (mappedByteBuffer != null) {
                unmapBuffer(mappedByteBuffer);
                mappedByteBuffer = null;
            }
            if (fileChannel != null) {
                fileChannel = null;
            }
            mapLength = 0L;
        }

        @Override
        public void close() throws Exception
        {
            unmap();
            if (randomAccessFile != null) {
                randomAccessFile.close();
                randomAccessFile = null;
            }
        }
    }

    public static class RandomAccessSliceFileWriter
        extends RandomAccessSliceFileReader
    {
        protected long writePosition;

        public RandomAccessSliceFileWriter(File file)
        {
            super(file, "rw", FileChannel.MapMode.READ_WRITE, 0L, Optional.empty()); // FIXME rws?
        }

        public void write(Slice slice)
        {
            long requiredLength = writePosition + slice.length();
            if (requiredLength >= mapLength) {
                grow(requiredLength - mapLength);
            }
            byte[] bytes = slice.getBytes(); // fixme bleh
            mappedByteBuffer.position((int) writePosition);
            mappedByteBuffer.put(bytes, 0, bytes.length);
            writePosition += bytes.length;
        }

        private void grow(long by)
        {
            long newLength = 1L << (64 - Long.numberOfLeadingZeros(mapLength + by));
            map(newLength);
        }
    }

    /*
    public static class FixedWidthRandomAccessSliceFileReader
            implements AutoCloseable
    {
        private final File dataFile;

        public FixedWidthRandomAccessSliceFileReader(File dataFile)
        {
            this.dataFile = dataFile;
        }

        @Override
        public void close() throws Exception
        {
        }
    }
    */

    /*
    public static class VariableWidthRandomAccessSliceFileWriter
            implements AutoCloseable
    {

    }

    public static class VariableWidthRandomAccessSliceFileReader
            implements AutoCloseable
    {

    }
    */

    @Test
    public void testStuff() throws Throwable
    {
        File f = File.createTempFile("foo", null);
        f.deleteOnExit();
        try (RandomAccessSliceFileWriter w = new RandomAccessSliceFileWriter(f)) {
            for (int i = 0; i < 1000; ++i) {
                w.write(Slices.wrappedBuffer((byte) 1, (byte) 2, (byte) 3));
            }
        }
    }
}
