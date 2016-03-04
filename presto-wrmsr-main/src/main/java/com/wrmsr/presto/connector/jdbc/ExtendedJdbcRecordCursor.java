/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.connector.jdbc;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcRecordCursor;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.BigintType;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.wrmsr.presto.util.jdbc.Queries;
import com.wrmsr.presto.util.collect.ImmutableCollectors;
import io.airlift.log.Logger;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class ExtendedJdbcRecordCursor
        extends JdbcRecordCursor
{
    private static final Logger log = Logger.get(ExtendedJdbcRecordCursor.class);

    private List<Integer> chunkPositionIndices;
    private List<Comparable<?>> chunkPositionValues;
    private List<String> clusteredColumnNames;
    private List<JdbcColumnHandle> clusteredColumnHandles;

    public ExtendedJdbcRecordCursor(JdbcClient jdbcClient, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        super(jdbcClient, split, columnHandles);
    }

    protected void begin()
    {
        String schemaName = isNullOrEmpty(split.getSchemaName()) ? split.getCatalogName() : split.getSchemaName(); // FIXME pile of hax growing
        try {
            try {
                clusteredColumnNames = Queries.getClusteredColumns(connection, schemaName, split.getTableName());
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            checkState(clusteredColumnNames.size() == 1); // FIXME

            JdbcTableHandle table = jdbcClient.getTableHandle(new SchemaTableName(schemaName, split.getTableName()));
            Map<String, JdbcColumnHandle> allColumns = jdbcClient.getColumns(table).stream().map(c -> ImmutablePair.of(c.getColumnName(), c)).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
            Map<String, Integer> cursorColumnIndexMap = IntStream.range(0, columnHandles.size()).boxed().map( // FIXME: helper
                    i -> ImmutablePair.of(columnHandles.get(i).getColumnName(), i)).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
            clusteredColumnHandles = clusteredColumnNames.stream().map(s -> allColumns.get(s)).collect(toImmutableList());

            chunkPositionIndices = newArrayList();
            for (String clusteredColumnName : clusteredColumnNames) {
                if (cursorColumnIndexMap.containsKey(clusteredColumnName)) {
                    chunkPositionIndices.add(cursorColumnIndexMap.get(clusteredColumnName));
                }
                else {
                    JdbcColumnHandle handle = allColumns.get(clusteredColumnName);
                    int idx = columnHandles.size();
                    chunkPositionIndices.add(idx);
                    cursorColumnIndexMap.put(clusteredColumnName, idx);
                    columnHandles.add(handle);
                }
            }

            advanceNextChunk();
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    private void advanceNextChunk()
    {
        try {
            TupleDomain<ColumnHandle> chunkPositionDomtain;
            if (chunkPositionValues == null) {
                chunkPositionDomtain = TupleDomain.all();
            }
            else {
                Map<ColumnHandle, Domain> domain = newHashMap();
                for (int i = 0; i < clusteredColumnHandles.size(); ++i) {
                    JdbcColumnHandle k = clusteredColumnHandles.get(i);
                    Range r = Range.greaterThan(BigintType.BIGINT, chunkPositionValues.get(i));
                    Domain v = Domain.create(ValueSet.ofRanges(r), false);
                    domain.put(k, v);
                }
                chunkPositionDomtain = TupleDomain.withColumnDomains(domain);
            }

            TupleDomain<ColumnHandle> chunkTupleDomain = split.getTupleDomain().intersect(chunkPositionDomtain);

            if (chunkTupleDomain.isNone()) {
                close();
                throw new RuntimeException(); // FIXME empty resultset runtimeerr's
            }

            JdbcSplit chunkSplit = new JdbcSplit(
                    split.getConnectorId(),
                    split.getCatalogName(),
                    split.getSchemaName(),
                    split.getTableName(),
                    split.getConnectionUrl(),
                    split.getConnectionProperties(),
                    chunkTupleDomain);

            statement = jdbcClient.buildSql(
                    chunkSplit,
                    columnHandles,
                    clusteredColumnHandles.stream().map(c -> new JdbcClient.Ordering(c, true)).collect(toImmutableList()),
                    Optional.of(1000L));

            statement.setFetchSize(1000);

            log.debug(String.format("Executing: %s", statement.toString()));
            resultSet = statement.executeQuery();
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    private void extractChunkPosition()
    {
        chunkPositionValues = chunkPositionIndices.stream().map((i) -> {
            try {
                Comparable<?> obj = (Comparable<?>) resultSet.getObject(i + 1);
                if (obj instanceof Integer) {
                    obj = new Long((Integer) obj);
                }
                return obj;
            }
            catch (SQLException e) {
                throw handleSqlException(e);
            }
        }).collect(Collectors.toList());
    }

    @Override
    public boolean advanceNextPosition()
    {
        return advanceNextPosition(true);
    }

    private boolean advanceNextPosition(boolean tryAdvanceChunk)
    {
        if (closed) {
            return false;
        }

        try {
            boolean result = resultSet.next();
            if (!result) {
                if (tryAdvanceChunk) {
                    advanceNextChunk();
                    return advanceNextPosition(false);
                }
                else {
                    return result;
                }
            }
            extractChunkPosition();
            return result;
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @SuppressWarnings({"UnusedDeclaration", "EmptyTryBlock"})
    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        // use try with resources to close everything properly
        try (Connection connection = this.connection;
                Statement statement = this.statement;
                ResultSet resultSet = this.resultSet) {
            // do nothing
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public String getIdentifierQuote()
    {
        return ((BaseJdbcClient) jdbcClient).getIdentifierQuote();
    }

    private String quote(String name)
    {
        String quote = getIdentifierQuote();
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }
}
