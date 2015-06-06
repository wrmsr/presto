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

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.inject.Inject;

import java.util.List;

public class ExtendedJdbcRecordSetProvider
        extends JdbcRecordSetProvider
{
    @Inject
    public ExtendedJdbcRecordSetProvider(JdbcClient jdbcClient)
    {
        super(jdbcClient);
    }

    @Override
    protected RecordSet createRecordSet(JdbcClient jdbcClient, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        return new ExtendedJdbcRecordSet(jdbcClient, split, columnHandles);
    }
}
