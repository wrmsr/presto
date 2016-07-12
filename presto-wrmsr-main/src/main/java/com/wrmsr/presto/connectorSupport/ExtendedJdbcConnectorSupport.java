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
package com.wrmsr.presto.connectorSupport;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.wrmsr.presto.connector.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.connector.jdbc.ExtendedJdbcConnector;
import com.wrmsr.presto.spi.connectorSupport.EvalConnectorSupport;
import com.wrmsr.presto.spi.connectorSupport.HandleDetailsConnectorSupport;
import com.wrmsr.presto.spi.connectorSupport.KeyConnectorSupport;
import com.wrmsr.presto.util.jdbc.ScriptRunner;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static com.google.common.base.Preconditions.checkArgument;

public class ExtendedJdbcConnectorSupport
        implements HandleDetailsConnectorSupport, KeyConnectorSupport, EvalConnectorSupport
{
    private final ConnectorSession session;
    private final ExtendedJdbcConnector connector;

    public ExtendedJdbcConnectorSupport(ConnectorSession session, Connector connector)
    {
        this.session = session;
        this.connector = (ExtendedJdbcConnector) connector;
        ;
    }

    public ExtendedJdbcClient getClient()
    {
        return (ExtendedJdbcClient) connector.getJdbcClient();
    }

    @Override
    public SchemaTableName getSchemaTableName(ConnectorTableHandle handle)
    {
        checkArgument(handle instanceof JdbcTableHandle);
        return ((JdbcTableHandle) handle).getSchemaTableName();
    }

    @Override
    public String getColumnName(ColumnHandle columnHandle)
    {
        return ((JdbcColumnHandle) columnHandle).getColumnName();
    }

    @Override
    public Type getColumnType(ColumnHandle columnHandle)
    {
        return ((JdbcColumnHandle) columnHandle).getColumnType();
    }

    @Override
    public List eval(String cmd)
    {
        return null;
    }

    @Override
    public void exec(String buf)
    {
        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_UNCOMMITTED, true);
        JdbcMetadata metadata = (JdbcMetadata) connector.getMetadata(transaction);
        connector.commit(transaction);
        BaseJdbcClient client = (BaseJdbcClient) metadata.getJdbcClient();
        try (Connection connection = client.getConnection()) {
            ScriptRunner scriptRunner = new ScriptRunner(connection);
            scriptRunner.runScript(new StringReader(buf));
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<Key> getKeys(SchemaTableName schemaTableName)
    {
        try {
            try (Connection connection = getClient().getConnection()) {
                DatabaseMetaData metadata = connection.getMetaData();

                // FIXME postgres catalog support
                try (ResultSet resultSet = metadata.getPrimaryKeys(schemaTableName.getSchemaName(), schemaTableName.getSchemaName(), schemaTableName.getTableName())) {
                    while (resultSet.next()) {
                        System.out.println(resultSet);
                    }
                }

                throw new UnsupportedOperationException();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Connector getConnector()
    {
        return connector;
    }
}
