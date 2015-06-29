package com.wrmsr.presto;

import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

public class TestFileBuffers
{
    public static class RandomAccessSliceFileReader
            implements AutoCloseable
    {
        protected final File file;
        protected final String mode;
        protected final FileChannel.MapMode mapMode;
        protected final long mapPosition;

        protected long length;

        protected RandomAccessFile randomAccessFile;
        protected FileChannel fileChannel;
        protected MappedByteBuffer mappedByteBuffer;

        public RandomAccessSliceFileReader(File file)
        {
            this(file, "r", FileChannel.MapMode.READ_ONLY, 0L);
        }

        protected RandomAccessSliceFileReader(File file, String mode, FileChannel.MapMode mapMode, long mapPosition)
        {
            this.file = file;
            this.mode = mode;
            this.mapMode = mapMode;
            this.mapPosition = mapPosition;
        }

        protected void open()
        {
            try {
                randomAccessFile = new RandomAccessFile(file, mode);
                fileChannel = randomAccessFile.getChannel();
                mappedByteBuffer = fileChannel.map(mapMode, mapPosition, length);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close() throws Exception
        {
        }
    }

    public static class RandomAccessSliceFileWriter
        extends RandomAccessSliceFileReader
    {
        protected int position;

        public RandomAccessSliceFileWriter(File file)
        {
            super(file, "rw", FileChannel.MapMode.READ_WRITE, 0L); // FIXME rws?
        }

        public void write(Slice slice)
        {
            long requiredLength = slice.length() + position;
            if (requiredLength >= length) {
                grow(requiredLength - length);
            }

        }

        private void grow(long by)
        {

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

    }
}
