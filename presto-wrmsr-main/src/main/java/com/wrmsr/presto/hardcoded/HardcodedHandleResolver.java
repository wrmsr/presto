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
package com.wrmsr.presto.hardcoded;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;

public class HardcodedHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return false;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return false;
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return false;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return null;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return null;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return null;
    }
}
