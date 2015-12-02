package com.wrmsr.presto.codec;

import com.wrmsr.presto.util.codec.Codec;

public interface Serializer
    extends Codec<Object, byte[]>
{
    String getName();
}
