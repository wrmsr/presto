package com.wrmsr.presto.codec;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ParametricType;
import com.wrmsr.presto.type.ParametricTypeRegistration;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.wrmsr.presto.codec.EncodedType.NAME;

public class EncodedParametricType
    implements ParametricType, ParametricTypeRegistration.Self
{
    private final TypeCodecManager typeCodecManager;

    @Inject
    public EncodedParametricType(TypeCodecManager typeCodecManager)
    {
        this.typeCodecManager = typeCodecManager;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Type createType(List<Type> types, List<Object> literals)
    {
        checkArgument(types.size() == 1);
        checkArgument(literals.size() == 1);
        checkArgument(literals.get(0) instanceof String);
        Type fromType = types.get(0);
        String codecName = (String) literals.get(0);
        TypeCodec typeCodec = typeCodecManager.getTypeCodec(codecName, fromType).get();
        return new EncodedType(typeCodec);
    }
}
