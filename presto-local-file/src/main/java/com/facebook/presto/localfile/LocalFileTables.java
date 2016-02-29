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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.localfile.LocalFileMetadata.PRESTO_LOGS_SCHEMA;
import static com.facebook.presto.localfile.LocalFileMetadata.SERVER_ADDRESS_COLUMN;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class LocalFileTables
{
    public static class HttpRequestLogTable
    {
        private static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
                SERVER_ADDRESS_COLUMN,
                new ColumnMetadata("timestamp", TIMESTAMP),
                new ColumnMetadata("client_address", VARCHAR),
                new ColumnMetadata("method", VARCHAR),
                new ColumnMetadata("request_uri", VARCHAR),
                new ColumnMetadata("user", VARCHAR),
                new ColumnMetadata("agent", VARCHAR),
                new ColumnMetadata("response_code", BIGINT),
                new ColumnMetadata("request_size", BIGINT),
                new ColumnMetadata("response_size", BIGINT),
                new ColumnMetadata("time_to_last_byte", BIGINT),
                new ColumnMetadata("trace_token", VARCHAR));

        private static final String TABLE_NAME = "http_request_log";

        public static List<ColumnMetadata> getColumns()
        {
            return COLUMNS;
        }

        public static SchemaTableName getSchemaTableName()
        {
            return new SchemaTableName(PRESTO_LOGS_SCHEMA, TABLE_NAME);
        }
    }
}
