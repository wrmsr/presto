package com.wrmsr.presto.flat;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class FlatRecordSet
    implements RecordSet
{
    public List<Type> getColumnTypes()
    {
        return ImmutableList.of(FlatMetadata.COLUMN_TYPE);
    }

    @Override
    public RecordCursor cursor()
    {
        return new FlatRecordCursor();
    }
}
