package com.wrmsr.presto.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcRecordSet;
import com.facebook.presto.plugin.jdbc.JdbcSplit;

import java.util.List;

public class ChunkedJdbcRecordSet
    extends JdbcRecordSet
{
    public ChunkedJdbcRecordSet(JdbcClient jdbcClient, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        super(jdbcClient, split, columnHandles);
    }
}
