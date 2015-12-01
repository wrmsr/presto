package com.wrmsr.presto.serialization;

import com.wrmsr.presto.util.codec.Codec;

public interface Serializer
    extends Codec<Object, byte[]>
{
    String getName();
}
