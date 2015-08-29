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
package com.wrmsr.presto.flat;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class FlatConnector
        implements Connector
{
    private final ConnectorHandleResolver connectorHandleResolver;
    private final ConnectorMetadata connectorMetadata;
    private final ConnectorSplitManager connectorSplitManager;
    private final ConnectorRecordSetProvider connectorRecordSetProvider;
    private final ConnectorRecordSinkProvider connectorRecordSinkProvider;

    @Inject
    public FlatConnector(ConnectorHandleResolver connectorHandleResolver, ConnectorMetadata connectorMetadata, ConnectorSplitManager connectorSplitManager, ConnectorRecordSetProvider connectorRecordSetProvider, ConnectorRecordSinkProvider connectorRecordSinkProvider)
    {
        this.connectorHandleResolver = checkNotNull(connectorHandleResolver);
        this.connectorMetadata = checkNotNull(connectorMetadata);
        this.connectorSplitManager = checkNotNull(connectorSplitManager);
        this.connectorRecordSetProvider = connectorRecordSetProvider;
        this.connectorRecordSinkProvider = connectorRecordSinkProvider;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return connectorHandleResolver;
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return connectorMetadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return connectorSplitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return connectorRecordSetProvider;
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return connectorRecordSinkProvider;
    }
}
