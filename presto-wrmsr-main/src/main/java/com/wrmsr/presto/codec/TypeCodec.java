package com.wrmsr.presto.codec;

import com.facebook.presto.spi.type.Type;
import com.wrmsr.presto.util.codec.Codec;
import io.airlift.slice.Slice;

public abstract class TypeCodec
{
    protected final String name;

    public TypeCodec(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public abstract <T> Codec<T, Slice> getCodec(Type fromType);
}
