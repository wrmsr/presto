package com.wrmsr.presto.struct;

import com.facebook.presto.type.RowType;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.wrmsr.presto.util.Box;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RowTypeSerializer
        extends StdSerializer<Box<List>>
{
    private final RowType rowType;

    public RowTypeSerializer(RowType rowType, Class listBoxClass)
    {
        super(listBoxClass);
        this.rowType = rowType;
    }

    @Override
    public void serialize(Box<List> value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException
    {
        checkNotNull(value);
        List list = value.getValue();
        if (list == null) {
            jgen.writeNull();
            return;
        }
        List<RowType.RowField> rowFields = rowType.getFields();
        checkArgument(list.size() == rowFields.size());
        jgen.writeStartObject();
        for (int i = 0; i < list.size(); ++i) {
            RowType.RowField rowField = rowFields.get(i);
            // FIXME nameless = lists
            jgen.writeObjectField(rowField.getName().get(), list.get(i));
        }
        jgen.writeEndObject();
    }
}
