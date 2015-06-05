package com.wrmsr.presto.hardcoded;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;

import java.util.List;

public class HardcodedRecordSetProvider
    implements ConnectorRecordSetProvider
{
    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        return null;
    }
}
