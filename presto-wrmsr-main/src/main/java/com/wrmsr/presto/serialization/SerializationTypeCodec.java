package com.wrmsr.presto.serialization;

import com.facebook.presto.spi.type.Type;
import com.wrmsr.presto.codec.TypeCodec;
import com.wrmsr.presto.util.codec.Codec;

public class SerializationTypeCodec
    extends TypeCodec
{
    public SerializationTypeCodec(String name, Type fromType, Type toType, Codec<Object, Object> codec)
    {
        super(name, fromType, toType, codec);
    }
}
