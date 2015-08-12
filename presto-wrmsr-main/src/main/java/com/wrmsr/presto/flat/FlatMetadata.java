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
package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkNotNull;

public class FlatMetadata
    implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "data";
    public static final String TABLE_NAME = "data";
    public static final String COLUMN_NAME = "data";
    public static final Type COLUMN_TYPE = VARBINARY;

    public static final SchemaTableName SCHEMA_TABLE_NAME = new SchemaTableName(SCHEMA_NAME, TABLE_NAME);

    public static final ColumnMetadata COLUMN_METADATA = new ColumnMetadata(
            COLUMN_NAME,
            COLUMN_TYPE,
            false,
            null,
            false
    );

    public static final ConnectorTableMetadata TABLE_METADATA = new ConnectorTableMetadata(
            SCHEMA_TABLE_NAME,
            ImmutableList.of(COLUMN_METADATA),
            ImmutableMap.of(),
            null,
            false
    );

    private final String connectorId;

    @Inject
    public FlatMetadata(FlatConnectorId connectorId)
    {
        this.connectorId = checkNotNull(connectorId).toString();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        if (SCHEMA_TABLE_NAME.equals(tableName)) {
            return new FlatTableHandle(connectorId);
        }
        else {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session,ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        if (tableHandle instanceof FlatTableHandle) {
            return TABLE_METADATA;
        }
        else {
            return null;
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (SCHEMA_NAME.equals(schemaNameOrNull)) {
            return ImmutableList.of(SCHEMA_TABLE_NAME);
        }
        else {
            return ImmutableList.of();
        }
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session,ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return false;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        if (tableHandle instanceof FlatTableHandle) {
            return ImmutableMap.of(COLUMN_NAME, new FlatColumnHandle(connectorId));
        }
        else {
            return null;
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        if (tableHandle instanceof FlatTableHandle && columnHandle instanceof FlatColumnHandle) {
            return COLUMN_METADATA;
        }
        else {
            return null;
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        // FIXME
        return ImmutableMap.of(SCHEMA_TABLE_NAME, TABLE_METADATA.getColumns());
    }

    // -- RO --

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public void renameTable(ConnectorSession session,ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public void renameColumn(ConnectorSession session,ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void dropTable(ConnectorSession session,ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public void commitCreateTable(ConnectorSession session,ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new FlatInsertTableHandle(connectorId);
    }

    @Override
    public void commitInsert(ConnectorSession session,ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        // throw new UnsupportedOperationException();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating views");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping views");
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }
}
