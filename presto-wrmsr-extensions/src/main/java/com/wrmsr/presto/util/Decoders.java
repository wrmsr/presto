package com.wrmsr.presto.util;

import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.util.List;

public class Decoders
{
    private Decoders()
    {
    }

    public interface CodecField
    {
        String getName();
        Type getType();
    }

    public interface CodecSchema
    {
        List<CodecField> getFields();
    }

    public interface RowDecoder
    {

    }

    public interface FieldDecoder
    {
        Type getType(int field);

        boolean advanceNextPosition();

        boolean getBoolean(int field);

        long getLong(int field);

        double getDouble(int field);

        Slice getSlice(int field);

        boolean isNull(int field);
    }
}
