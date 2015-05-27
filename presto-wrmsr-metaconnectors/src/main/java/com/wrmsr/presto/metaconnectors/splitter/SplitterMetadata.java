package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Created by wtimoney on 5/26/15.
 */
public class SplitterMetadata
        extends ReadOnlyConnectorMetadata
{
        private final String connectorId;

        public SplitterMetadata(String connectorId)
        {
                this.connectorId = connectorId;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session)
        {
                return null;
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
                return null;
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
        public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
        {
                return null;
        }

        @Override
        public ConnectorTableLayout getTableLayout(ConnectorTableLayoutHandle handle)
        {
                return null;
        }

        @Override
        public void rollbackCreateTable(ConnectorOutputTableHandle tableHandle)
        {

        }

        @Override
        public void rollbackInsert(ConnectorInsertTableHandle insertHandle)
        {

        }
}
