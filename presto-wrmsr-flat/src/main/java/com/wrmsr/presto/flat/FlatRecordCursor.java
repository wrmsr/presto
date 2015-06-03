package com.wrmsr.presto.flat;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.io.IOException;

public class FlatRecordCursor
    implements RecordCursor
{

    public FlatRecordCursor()
    {
        try {


        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return null;
    }

    @Override
    public boolean advanceNextPosition()
    {
        return false;
    }

    @Override
    public boolean getBoolean(int field)
    {
        return false;
    }

    @Override
    public long getLong(int field)
    {
        return 0;
    }

    @Override
    public double getDouble(int field)
    {
        return 0;
    }

    @Override
    public Slice getSlice(int field)
    {
        return null;
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public void close()
    {

    }
}
