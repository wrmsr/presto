package com.wrmsr.presto.serialization;

import com.facebook.presto.spi.type.Type;
import com.wrmsr.presto.util.codec.Codec;

public abstract class TypeCodec
{
    protected final Type fromType;
    protected final Type toType;
    protected final Codec<?, ?> codec;

    public TypeCodec(Type fromType, Type toType, Codec<?, ?> codec)
    {
        this.fromType = fromType;
        this.toType = toType;
        this.codec = codec;
    }
}
