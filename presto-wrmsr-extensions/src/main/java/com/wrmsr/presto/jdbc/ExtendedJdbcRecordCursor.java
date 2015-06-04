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

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcRecordCursor;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ExtendedJdbcRecordCursor
        extends JdbcRecordCursor
{
    public ExtendedJdbcRecordCursor(JdbcClient jdbcClient, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        super(jdbcClient, split, columnHandles);
        try {
            Connection connection = getConnection();

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
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
