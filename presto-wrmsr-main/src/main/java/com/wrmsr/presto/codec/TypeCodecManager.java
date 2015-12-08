package com.wrmsr.presto.codec;

import com.facebook.presto.spi.type.Type;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

public class TypeCodecManager
    implements TypeCodecProvider
{
    private final Set<TypeCodecProvider> typeCodecProviders;

    @Inject
    public TypeCodecManager(Set<TypeCodecProvider> typeCodecProviders)
    {
        this.typeCodecProviders = typeCodecProviders;
    }

    public Optional<TypeCodec> getTypeCodec(String name, Type fromType)
    {
        throw new UnsupportedOperationException();
    }
}
