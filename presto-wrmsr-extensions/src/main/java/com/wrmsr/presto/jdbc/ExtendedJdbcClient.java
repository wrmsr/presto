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
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.wrmsr.presto.jdbc.util.ScriptRunner;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import static com.google.common.collect.Maps.fromProperties;
import static java.util.Locale.ENGLISH;

public class ExtendedJdbcClient
        extends BaseJdbcClient
{
    protected final ExtendedJdbcConfig extendedConfig;

    public ExtendedJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config, ExtendedJdbcConfig extendedConfig, String identifierQuote, Driver driver)
    {
        super(connectorId, config, identifierQuote, driver);
        this.extendedConfig = extendedConfig;
    }

    public void executeScript(String sql)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            ScriptRunner scriptRunner = new ScriptRunner(connection);
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(JdbcPartition jdbcPartition)
    {
        JdbcTableHandle jdbcTableHandle = jdbcPartition.getJdbcTableHandle();
        ExtendedJdbcSplit jdbcSplit = new ExtendedJdbcSplit(
                connectorId,
                jdbcTableHandle.getCatalogName(),
                jdbcTableHandle.getSchemaName(),
                jdbcTableHandle.getTableName(),
                connectionUrl,
                fromProperties(connectionProperties),
                jdbcPartition.getTupleDomain(),
                isRemotelyAccessible());
        return new FixedSplitSource(connectorId, ImmutableList.of(jdbcSplit));
    }
}
