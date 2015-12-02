package com.wrmsr.presto.codec;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ParametricType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class SerializedParametricType
    implements ParametricType
{
    private final SerializationManager serializationManager;

    public SerializedParametricType(SerializationManager serializationManager)
    {
        this.serializationManager = serializationManager;
    }

    @Override
    public String getName()
    {
        return SerializedType.NAME;
    }

    @Override
    public Type createType(List<Type> types, List<Object> literals)
    {
        checkArgument(types.size() == 1);
        checkArgument(literals.size() == 1);
        checkArgument(literals.get(0) instanceof String);
        String serializerName = (String) literals.get(0);
        Serializer serializer = serializationManager.getSerializer(serializerName);
        return new SerializedType(types.get(0), serializer);
    }
}
