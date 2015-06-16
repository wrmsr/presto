package com.wrmsr.presto.jdbc;

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.wrmsr.presto.jdbc.util.Queries;
import com.wrmsr.presto.metaconnectors.partitioner.Partitioner;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class JdbcPartitioner implements Partitioner
{
    private final Connection connection;
    private final Function<String, String> quote;

    public JdbcPartitioner(Connection connection, Function<String, String> quote)
    {
        this.connection = connection;
        this.quote = quote;
    }

    @Override
    public List<ConnectorPartition> getPartitionsConnector(SchemaTableName table, TupleDomain<ColumnHandle> tupleDomain)
    {
        List<String> clusteredColumnNames = Queries.getClusteredColumns(
                connection,
                null,
                table.getSchemaName(),
                table.getTableName());
        Map<String, Queries.ColumnDomain> clusteredColumnDomains = Queries.getColumnDomains(
                connection,
                null,
                table.getSchemaName(),
                table.getTableName(),
                clusteredColumnNames,
                quote);
        return null;
    }
}
