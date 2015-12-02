package com.wrmsr.presto.codec;

import com.facebook.presto.spi.type.Type;
import com.wrmsr.presto.util.codec.Codec;

public abstract class TypeCodec
{
    protected final String name;
    protected final Type fromType;
    protected final Type toType;
    protected final Codec<Object, Object> codec;

    public TypeCodec(String name, Type fromType, Type toType, Codec<Object, Object> codec)
    {
        this.name = name;
        this.fromType = fromType;
        this.toType = toType;
        this.codec = codec;
    }

    public String getName()
    {
        return name;
    }

    public Type getFromType()
    {
        return fromType;
    }

    public Type getToType()
    {
        return toType;
    }

    public Codec<Object, Object> getCodec()
    {
        return codec;
    }
}
