package com.wrmsr.presto.codec;

import com.facebook.presto.spi.type.Type;
import com.sun.corba.se.impl.encoding.TypeCodeOutputStream;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

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
        Optional<TypeCodec> ret = Optional.empty();
        for (TypeCodecProvider tpv : typeCodecProviders) {
            Optional<TypeCodec> cur = tpv.getTypeCodec(name, fromType);
            if (cur.isPresent()) {
                checkState(!ret.isPresent());
                ret = cur;
            }
        }
        return ret;
    }
}
