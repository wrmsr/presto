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
package com.wrmsr.presto.connector.views;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class ViewsConnectorMetadata
    implements ConnectorMetadata
{
    private final ViewStorage viewStorage;

    public ViewsConnectorMetadata(ViewStorage viewStorage)
    {
        this.viewStorage = viewStorage;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of("views");
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        checkArgument(viewName.getSchemaName().equals("views"));
        viewStorage.putView(viewName.getTableName(), viewData);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        checkArgument(viewName.getSchemaName().equals("views"));
        viewStorage.dropView(viewName.getTableName());
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull != null) {
            checkArgument(schemaNameOrNull.equals("views"));
        }
        return viewStorage.getViewNames().stream().map(n -> new SchemaTableName("views", n)).collect(toImmutableList());
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkArgument(prefix.getSchemaName().equals("views"));
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listViews(session, prefix.getSchemaName());
        }

        for (SchemaTableName schemaTableName : tableNames) {
            views.put(schemaTableName, new ConnectorViewDefinition(
                        schemaTableName,
                        Optional.empty(),
                        viewStorage.getView(schemaTableName.getTableName())));
        }

        return views.build();
    }
}
