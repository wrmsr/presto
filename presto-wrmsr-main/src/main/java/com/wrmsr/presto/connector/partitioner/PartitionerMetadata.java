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
package com.wrmsr.presto.connector.partitioner;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.util.Types.checkType;

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
                metadata.getComment(),
                metadata.isHidden()
        );
    }

    private ConnectorTableMetadata setPartitioning(ConnectorTableMetadata metadata)
    {
        return new ConnectorTableMetadata(
                metadata.getTable(),
                metadata.getColumns().stream().map(this::setPartitioning).collect(Collectors.toList()),
                metadata.getProperties(),
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
        return new PartitionerTableHandle(target.getTableHandle(session, tableName));
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        PartitionerTableHandle tableHandle = checkType(table, PartitionerTableHandle.class, "table");
        List<ConnectorTableLayoutResult> out = target.getTableLayouts(session, table, constraint, desiredColumns);

//        ConnectorTableLayout layout = new ConnectorTableLayout(new JdbcTableLayoutHandle(tableHandle, constraint.getSummary()));
//        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
        // FIXME
        return target.getTableLayouts(session, table, constraint, desiredColumns);
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        // FIXME
        return target.getTableLayout(session, handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ConnectorTableHandle targetHandle = ((PartitionerTableHandle) table).getTarget(); // FIXME ... the rest of these ...
        return setPartitioning(target.getTableMetadata(session, targetHandle));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return target.listTables(session, schemaNameOrNull);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return target.getSampleWeightColumnHandle(session, tableHandle);
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return target.canCreateSampledTables(session);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return target.getColumnHandles(session, tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return setPartitioning(target.getColumnMetadata(session, tableHandle, columnHandle));
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
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        target.dropTable(session, tableHandle);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        target.renameTable(session, tableHandle, newTableName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        target.addColumn(session, tableHandle, column);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        this.target.renameColumn(session, tableHandle, source, target);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return target.getNewTableLayout(session, tableMetadata);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        return target.beginCreateTable(session, tableMetadata, layout);
    }

    @Override
    public void finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        target.finishCreateTable(session, tableHandle, fragments);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return target.beginInsert(session, tableHandle);
    }

    @Override
    public void finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        target.finishInsert(session, insertHandle, fragments);
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return target.getUpdateRowIdColumnHandle(session, tableHandle);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return target.beginDelete(session, tableHandle);
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        target.finishDelete(session, tableHandle, fragments);
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
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return target.getViews(session, prefix);
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return target.supportsMetadataDelete(session, tableHandle, tableLayoutHandle);
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return target.metadataDelete(session, tableHandle, tableLayoutHandle);
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        return target.resolveIndex(session, tableHandle, indexableColumns, outputColumns, tupleDomain);
    }
}
