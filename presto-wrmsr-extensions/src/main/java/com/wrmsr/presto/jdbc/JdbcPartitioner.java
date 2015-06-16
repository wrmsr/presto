package com.wrmsr.presto.jdbc;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Throwables;
import com.wrmsr.presto.jdbc.util.Queries;
import com.wrmsr.presto.metaconnectors.partitioner.Partitioner;
import com.wrmsr.presto.util.ColumnDomain;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newHashSet;

public class JdbcPartitioner implements Partitioner
{
    public static final int NUM_PARTITIONS = 10; // FIXME

    private final Supplier<Connection> connectionSupplier;
    private final Function<String, String> quote;

    public JdbcPartitioner(Supplier<Connection> connectionSupplier, Function<String, String> quote)
    {
        this.connectionSupplier = connectionSupplier;
        this.quote = quote;
    }

    @Override
    public List<Partition> getPartitionsConnector(SchemaTableName table, TupleDomain<String> tupleDomain)
    {
        try (Connection connection = connectionSupplier.get()) {
            List<String> clusteredColumnNames = Queries.getClusteredColumns(
                    connection,
                    null,
                    table.getSchemaName(),
                    table.getTableName());
            Map<String, ColumnDomain> clusteredColumnDomains = Queries.getColumnDomains(
                    connection,
                    null,
                    table.getSchemaName(),
                    table.getTableName(),
                    clusteredColumnNames,
                    quote);
            checkState(newHashSet(clusteredColumnNames).equals(clusteredColumnDomains.keySet()));
            LinkedHashMap<String, ColumnDomain> linkedClusteredColumnDomains = new LinkedHashMap<>();
            for (String column : clusteredColumnNames) {
                linkedClusteredColumnDomains.put(column, clusteredColumnDomains.get(column));
            }
            return Partitioner.generateDensePartitions(linkedClusteredColumnDomains, NUM_PARTITIONS);
        }
        catch (SQLException | IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
