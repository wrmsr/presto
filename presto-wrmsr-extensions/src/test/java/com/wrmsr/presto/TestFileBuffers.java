package com.wrmsr.presto;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class TestFileBuffers
{
    public static class RandomAccessSliceFileReader
            implements AutoCloseable
    {
        protected final File dataFile;

        protected int length;

        public RandomAccessSliceFileReader(File dataFile)
        {
            this.dataFile = dataFile;
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

        public RandomAccessSliceFileWriter(File dataFile)
        {
            super(dataFile);
        }

        public void write(Slice slice)
        {
            int requiredLength = slice.length() + position;
            if (requiredLength >= length) {
                grow(requiredLength - length);
            }

        }

        private void grow(int by)
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
