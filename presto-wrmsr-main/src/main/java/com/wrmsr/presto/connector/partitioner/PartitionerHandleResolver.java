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
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class PartitionerHandleResolver
    implements ConnectorHandleResolver
{
    private final ConnectorHandleResolver target;

    public PartitionerHandleResolver(ConnectorHandleResolver target)
    {
        this.target = target;
    }

    public ConnectorHandleResolver getTarget()
    {
        return target;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return target.getColumnHandleClass();
    }

    @Override
    public Class<? extends ConnectorIndexHandle> getIndexHandleClass()
    {
        return target.getIndexHandleClass();
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        return target.getInsertTableHandleClass();
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        return target.getOutputTableHandleClass();
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return target.getSplitClass();
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return PartitionerTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        return target.getTableLayoutHandleClass();
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        return target.getTransactionHandleClass();
    }
}
