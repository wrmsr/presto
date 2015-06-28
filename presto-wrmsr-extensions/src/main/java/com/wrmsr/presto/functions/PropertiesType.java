package com.wrmsr.presto.functions;

import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.MapType;

import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;

public class PropertiesType
    extends MapType
{
    public static final PropertiesType PROPERTIES = new PropertiesType();

    public PropertiesType()
    {
        super(parameterizedTypeName("properties"), VarcharType.VARCHAR, VarcharType.VARCHAR);
    }
}
