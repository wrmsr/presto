package com.wrmsr.presto.codec;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ParametricType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class EncodedParametricType
        implements ParametricType
{
    private final TypeCodec typeCodec;

    public EncodedParametricType(TypeCodec typeCodec)
    {
        this.typeCodec = typeCodec;
    }

    @Override
    public String getName()
    {
        return typeCodec.getName();
    }

    @Override
    public EncodedType createType(List<Type> types, List<Object> literals)
    {
        checkArgument(literals.size() == 0);
        checkArgument(types.size() == 1);
        Type fromType = types.get(0);
        return new EncodedType(typeCodec, fromType);
    }
}
