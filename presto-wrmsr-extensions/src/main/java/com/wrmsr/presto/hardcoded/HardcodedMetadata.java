package com.wrmsr.presto.hardcoded;

import com.facebook.presto.metadata.ViewDefinition;
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
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class HardcodedMetadata
    implements ConnectorMetadata
{
    // private final Map<String, Map<String, String>> viewsBySchemaByTable

    @Override
    public final List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    @Override
    public final Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table)
    {
        return null;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return ImmutableList.of();
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return null;
    }

    @Override
    public final void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public final void renameTable(ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public void renameColumn(ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public final void dropTable(ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    @Override
    public final boolean canCreateSampledTables(ConnectorSession session)
    {
        return false;
    }

    @Override
    public final ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public final void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    @Override
    public final void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating views");
    }

    @Override
    public final void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping views");
    }
}

