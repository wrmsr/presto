package com.wrmsr.presto.flat;

import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;

public class FlatRecordSink
    implements RecordSink
{
    @Override
    public void beginRecord(long sampleWeight)
    {

    }

    @Override
    public void finishRecord()
    {

    }

    @Override
    public void appendNull()
    {

    }

    @Override
    public void appendBoolean(boolean value)
    {

    }

    @Override
    public void appendLong(long value)
    {

    }

    @Override
    public void appendDouble(double value)
    {

    }

    @Override
    public void appendString(byte[] value)
    {

    }

    @Override
    public Collection<Slice> commit()
    {
        return null;
    }

    @Override
    public void rollback()
    {

    }

    @Override
    public List<Type> getColumnTypes()
    {
        return null;
    }
}
