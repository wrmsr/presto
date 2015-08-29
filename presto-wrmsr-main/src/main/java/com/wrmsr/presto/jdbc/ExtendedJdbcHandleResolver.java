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

import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.google.inject.Inject;

public class ExtendedJdbcHandleResolver
        extends JdbcHandleResolver
{
    @Inject
    public ExtendedJdbcHandleResolver(JdbcConnectorId clientId)
    {
        super(clientId);
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return ExtendedJdbcSplit.class;
    }
}
