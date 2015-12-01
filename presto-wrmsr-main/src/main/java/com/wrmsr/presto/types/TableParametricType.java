package com.wrmsr.presto.types;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ParametricType;

import java.util.List;

import static com.wrmsr.presto.types.TableType.NAME;

public class TableParametricType
    implements ParametricType
{
    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Type createType(List<Type> types, List<Object> literals)
    {
        return null;
    }
}
