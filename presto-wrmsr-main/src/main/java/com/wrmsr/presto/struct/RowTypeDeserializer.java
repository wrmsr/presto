package com.wrmsr.presto.struct;

import com.facebook.presto.type.RowType;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.wrmsr.presto.util.Box;
import io.airlift.slice.Slice;

import java.io.IOException;

public class RowTypeDeserializer
        extends StdDeserializer<Box<Slice>>
{
    private final RowType rowType;

    public RowTypeDeserializer(RowType rowType, Class sliceBoxClass)
    {
        super(sliceBoxClass);
        this.rowType = rowType;
    }

    @Override
    public Box<Slice> deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException
    {
        return null;
    }
}
