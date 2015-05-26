package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;

import java.util.List;

// import static com.facebook.presto.tpch.Types.checkType;

/**
 * Created by wtimoney on 5/26/15.
 */
public class SplitterRecordSetProvider
        implements ConnectorRecordSetProvider
{
    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        throw new IllegalStateException();
        /*
        TpchSplit tpchSplit = checkType(split, TpchSplit.class, "split");

        String tableName = tpchSplit.getTableHandle().getTableName();

        TpchTable<?> tpchTable = TpchTable.getTable(tableName);

        return getRecordSet(tpchTable, columns, tpchSplit.getTableHandle().getScaleFactor(), tpchSplit.getPartNumber(), tpchSplit.getTotalParts());
        */
    }
}
