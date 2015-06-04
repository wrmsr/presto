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
package com.wrmsr.presto.jdbc;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcRecordCursor;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.QueryBuilder;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

public class ExtendedJdbcRecordCursor
        extends JdbcRecordCursor
{
    private static final List<String> MIN_AND_MAX = ImmutableList.of("MIN", "MAX");

    public ExtendedJdbcRecordCursor(JdbcClient jdbcClient, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        super(jdbcClient, split, columnHandles);
        try {
            String clusteredIndexName = null;
            Map<Integer, String> clusteredColumnsByOrdinal = Maps.newHashMap();
            DatabaseMetaData metadata = connection.getMetaData();

            try (ResultSet resultSet = metadata.getIndexInfo(split.getCatalogName(), split.getSchemaName(), split.getTableName(), false, false)) {
                while (resultSet.next()) {
                    if (resultSet.getShort("TYPE") != DatabaseMetaData.tableIndexClustered) {
                        continue;
                    }

                    String indexName = checkNotNull(resultSet.getString("INDEX_NAME"));
                    if (clusteredColumnsByOrdinal.isEmpty()) {
                        checkState(clusteredIndexName == null);
                        clusteredIndexName = indexName;
                    }
                    else {
                        checkState(indexName.equals(clusteredIndexName));
                    }

                    int ordinalPosition = resultSet.getInt("ORDINAL_POSITION");
                    String columnName = checkNotNull(resultSet.getString("COLUMN_NAME"));
                    // boolean isDescending = resultSet.getBoolean("ASC_OR_DESC"); // FIXME
                    checkState(!clusteredColumnsByOrdinal.containsKey(ordinalPosition));
                    clusteredColumnsByOrdinal.put(ordinalPosition, columnName);
                }
            }

            if (clusteredIndexName == null) {
                try (ResultSet resultSet = metadata.getPrimaryKeys(split.getCatalogName(), split.getSchemaName(), split.getTableName())) {
                    while (resultSet.next()) {
                        int ordinalPosition = resultSet.getInt("KEY_SEQ");
                        String columnName = checkNotNull(resultSet.getString("COLUMN_NAME"));
                        checkState(!clusteredColumnsByOrdinal.containsKey(ordinalPosition));
                        clusteredColumnsByOrdinal.put(ordinalPosition, columnName);
                    }
                }
            }

            List<String> clusteredColumns = IntStream.range(1, clusteredColumnsByOrdinal.size() + 1).boxed()
                    .map(i -> clusteredColumnsByOrdinal.get(i))
                    .collect(Collectors.toList());
            checkState(Sets.newHashSet(clusteredColumns).size() == clusteredColumns.size());

            {
                StringBuilder sql = new StringBuilder();
                sql.append("SELECT ");
                Joiner.on(", ").appendTo(sql, clusteredColumns.stream().flatMap(c -> MIN_AND_MAX.stream().map(f -> String.format("%s(%s)", f, quote(c)))).collect(Collectors.toList()));

                sql.append(" FROM ");
                if (!isNullOrEmpty(split.getCatalogName())) {
                    sql.append(quote(split.getCatalogName())).append('.');
                }
                if (!isNullOrEmpty(split.getSchemaName())) {
                    sql.append(quote(split.getSchemaName())).append('.');
                }
                sql.append(quote(split.getTableName()));

                try (
                        Statement idStatement = connection.createStatement();
                        ResultSet idResult = idStatement.executeQuery(sql.toString())) {
                    checkState(idResult.next());
                    checkState(idResult.getMetaData().getColumnCount() == clusteredColumns.size() * 2);
                    for (int i = 0; i < clusteredColumns.size(); ++i) {
                        System.out.println(clusteredColumns.get(i));
                        System.out.println(idResult.getInt((i * 2) + 1));
                        System.out.println(idResult.getInt((i * 2) + 2));
                    }
                }
            }

            {
                QueryBuilder qb = new QueryBuilder(getIdentifierQuote());
                StringBuilder sql = new StringBuilder(
                        qb.buildSql(
                                split.getCatalogName(),
                                split.getSchemaName(),
                                split.getTableName(),
                                columnHandles,
                                split.getTupleDomain().intersect(
                                        TupleDomain.withColumnDomains(
                                                ImmutableMap.of(
                                                        columnHandles.get(0), //FIXME)
                                                        Domain.create(SortedRangeSet.of(
                                                                // Range.equal(1000L)
                                                                Range.range(100L, true, 1000L, false)
                                                        ), false)
                                                )
                                        ))));
                sql.append(" ORDER BY ");
                Joiner.on(", ").appendTo(sql, clusteredColumns.stream().map(c -> quote(c) + " ASC").collect(Collectors.toList()));
                sql.append(" LIMIT ");
                sql.append(100);

                try (
                        Statement statement = connection.createStatement();
                        ResultSet result = statement.executeQuery(sql.toString())) {
                    while (result.next()) {
                        System.out.println(result);
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getIdentifierQuote()
    {
        return((BaseJdbcClient)jdbcClient).getIdentifierQuote();
    }

    private String quote(String name)
    {
        String quote = getIdentifierQuote();
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }
}
