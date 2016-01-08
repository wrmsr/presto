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
package com.wrmsr.presto.connector.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcConnector;
import com.facebook.presto.plugin.jdbc.JdbcHandleResolver;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcRecordSinkProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;

import static com.google.common.base.Preconditions.checkNotNull;

public class ExtendedJdbcConnector
        extends JdbcConnector
{
    private final JdbcClient jdbcClient;

    @Inject
    public ExtendedJdbcConnector(
            LifeCycleManager lifeCycleManager,
            JdbcMetadata jdbcMetadata,
            JdbcSplitManager jdbcSplitManager,
            JdbcRecordSetProvider jdbcRecordSetProvider,
            JdbcRecordSinkProvider jdbcRecordSinkProvider,
            JdbcClient jdbcClient)
    {
        super(lifeCycleManager, jdbcMetadata, jdbcSplitManager, jdbcRecordSetProvider, jdbcRecordSinkProvider);
        this.jdbcClient = checkNotNull(jdbcClient);
    }

    public JdbcClient getJdbcClient()
    {
        return jdbcClient;
    }
}
