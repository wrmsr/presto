package com.wrmsr.presto.serialization;

import com.facebook.presto.spi.type.Type;
import com.wrmsr.presto.codec.TypeCodec;
import com.wrmsr.presto.util.codec.Codec;
import io.airlift.slice.Slice;

public class SerializationTypeCodec
        extends TypeCodec
{
    public SerializationTypeCodec(String name)
    {
        super(name);
    }

    @Override
    public <T> Codec<T, Slice> getCodec(Type fromType)
    {
        return null;
    }
}
