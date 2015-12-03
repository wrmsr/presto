package com.wrmsr.presto.struct;

import com.facebook.presto.type.RowType;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public final class StructInfo
{
    private final RowType rowType;
    private final Class<?> sliceBoxClass;
    private final Class<?> listBoxClass;
    private final StdSerializer serializer;
    private final StdDeserializer deserializer;

    public StructInfo(RowType rowType, Class<?> sliceBoxClass, Class<?> listBoxClass, StdSerializer serializer, StdDeserializer deserializer)
    {
        this.rowType = rowType;
        this.sliceBoxClass = sliceBoxClass;
        this.listBoxClass = listBoxClass;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public String getName()
    {
        return rowType.getTypeSignature().getBase();
    }

    public RowType getRowType()
    {
        return rowType;
    }

    public Class<?> getSliceBoxClass()
    {
        return sliceBoxClass;
    }

    public Class<?> getListBoxClass()
    {
        return listBoxClass;
    }

    public StdSerializer getSerializer()
    {
        return serializer;
    }

    public StdDeserializer getDeserializer()
    {
        return deserializer;
    }
}
