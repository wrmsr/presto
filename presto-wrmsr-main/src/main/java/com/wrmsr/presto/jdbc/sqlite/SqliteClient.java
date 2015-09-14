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
package com.wrmsr.presto.jdbc.sqlite;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.google.inject.Inject;
import com.wrmsr.presto.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.jdbc.ExtendedJdbcConfig;

public class SqliteClient
        extends ExtendedJdbcClient
{
    @Inject
    public SqliteClient(JdbcConnectorId connectorId, BaseJdbcConfig config, ExtendedJdbcConfig extendedConfig)
    {
        super(connectorId, config, extendedConfig, "\"", createDriver(extendedConfig, () -> newDriver("org.sqlite.JDBC")));
    }
}
