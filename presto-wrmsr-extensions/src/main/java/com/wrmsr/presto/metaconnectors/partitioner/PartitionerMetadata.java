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
package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionerMetadata
    implements ConnectorMetadata
{
    private final ConnectorMetadata target;

    public PartitionerMetadata(ConnectorMetadata target)
    {
        this.target = target;
    }

    private ColumnMetadata setPartitioning(ColumnMetadata metadata)
    {
        if (!"id".equals(metadata.getName())) {
            return metadata;
        }

        return new ColumnMetadata(
                metadata.getName(),
                metadata.getType(),
                true,
                metadata.getComment(),
                metadata.isHidden()
        );
    }

    private ConnectorTableMetadata setPartitioning(ConnectorTableMetadata metadata)
    {
        return new ConnectorTableMetadata(
                metadata.getTable(),
                metadata.getColumns().stream().map(this::setPartitioning).collect(Collectors.toList()),
                metadata.getOwner(),
                metadata.isSampled()
        );
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return target.listSchemaNames(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return target.getTableHandle(session, tableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        // FIXME
        return target.getTableLayouts(table, constraint, desiredColumns);
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorTableLayoutHandle handle)
    {
        // FIXME
        return target.getTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table)
    {
        return setPartitioning(target.getTableMetadata(table));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return target.listTables(session, schemaNameOrNull);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return target.getSampleWeightColumnHandle(tableHandle);
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return target.canCreateSampledTables(session);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        return target.getColumnHandles(tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return setPartitioning(target.getColumnMetadata(tableHandle, columnHandle));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return Maps.transformValues(
                target.listTableColumns(session, prefix),
                l -> Lists.transform(l, this::setPartitioning));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        target.createTable(session, tableMetadata);
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        target.dropTable(tableHandle);
    }

    @Override
    public void renameTable(ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        target.renameTable(tableHandle, newTableName);
    }

    @Override
    public void renameColumn(ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        this.target.renameColumn(tableHandle, source, target);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return target.beginCreateTable(session, tableMetadata);
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        target.commitCreateTable(tableHandle, fragments);
    }

    @Override
    public void rollbackCreateTable(ConnectorOutputTableHandle tableHandle)
    {
        target.rollbackCreateTable(tableHandle);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return target.beginInsert(session, tableHandle);
    }

    @Override
    public void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        target.commitInsert(insertHandle, fragments);
    }

    @Override
    public void rollbackInsert(ConnectorInsertTableHandle insertHandle)
    {
        target.rollbackInsert(insertHandle);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        target.createView(session, viewName, viewData, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        target.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return target.listViews(session, schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return target.getViews(session, prefix);
    }
}
