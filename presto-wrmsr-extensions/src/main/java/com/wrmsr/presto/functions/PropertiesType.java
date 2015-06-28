package com.wrmsr.presto.functions;

import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.MapType;

public class PropertiesType
    extends MapType
{
    public static final PropertiesType PROPERTIES = new PropertiesType();

    public PropertiesType()
    {
        super(VarcharType.VARCHAR, VarcharType.VARCHAR);
    }
}
