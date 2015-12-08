package com.wrmsr.presto.codec;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public class TypeCodecManager
{
    private final Set<TypeCodec> typeCodecs;

    @Inject
    public TypeCodecManager(Set<TypeCodec> typeCodecs)
    {
        this.typeCodecs = typeCodecs;
    }

    public void addTypeCodec(TypeCodec typeCodec)
    {
    }

    public Optional<TypeCodec> getTypeCodec(String name)
    {
        throw new UnsupportedOperationException();
    }
}
