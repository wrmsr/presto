package com.wrmsr.presto.serialization;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ParametricType;

import java.util.List;

public class SerializedParametricType
    implements ParametricType
{
    @Override
    public String getName()
    {
        return SerializedType.NAME;
    }

    @Override
    public Type createType(List<Type> types, List<Object> literals)
    {
        return null;
    }
}
