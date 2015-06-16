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

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.jdbc.util.Queries;
import com.wrmsr.presto.util.ColumnDomain;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;

public class ExtendedJdbcRecordCursor
        extends JdbcRecordCursor
{
    public ExtendedJdbcRecordCursor(JdbcClient jdbcClient, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        super(jdbcClient, split, columnHandles);
        try {
            List<String> clusteredColumnNames = Queries.getClusteredColumns(
                    connection,
                    split.getSchemaName(),
                    split.getTableName());
            Map<String, ColumnDomain> clusteredColumnDomains = Queries.getColumnDomains(
                    connection,
                    split.getCatalogName(),
                    split.getSchemaName(),
                    split.getTableName(),
                    clusteredColumnNames,
                    this::quote);

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
                                                                Range.range(100L, true, 3100L, false)
                                                        ), false)
                                                )
                                        ))));
                sql.append(" ORDER BY ");
                Joiner.on(", ").appendTo(sql, clusteredColumnNames.stream().map(c -> quote(c) + " ASC").collect(Collectors.toList()));
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
        catch (SQLException | IOException e) {
            throw Throwables.propagate(e);
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
