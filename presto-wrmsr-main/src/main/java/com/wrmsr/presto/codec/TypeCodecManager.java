package com.wrmsr.presto.codec;

import com.facebook.presto.spi.type.Type;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

public class TypeCodecManager
    implements TypeCodecProvider
{
    private final Set<TypeCodec> typeCodecs;

    @Inject
    public TypeCodecManager(Set<TypeCodec> typeCodecs)
    {
        this.typeCodecs = typeCodecs;
    }

    public Optional<TypeCodec> getTypeCodec(String name, Type fromType)
    {
        throw new UnsupportedOperationException();
    }
}
