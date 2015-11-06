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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class FlatSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;

    @Inject
    public FlatSplitManager(FlatConnectorId connectorId)
    {
        this.connectorId = checkNotNull(connectorId).toString();
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorSession session, ConnectorTableHandle table, TupleDomain<ColumnHandle> tupleDomain)
    {
        checkArgument(table instanceof FlatTableHandle);

        return new ConnectorPartitionResult(
                ImmutableList.<ConnectorPartition>of(new FlatPartition((FlatTableHandle) table, tupleDomain)),
                tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorSession session, ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        checkArgument(table instanceof FlatTableHandle);

        return new FixedSplitSource(connectorId, ImmutableList.of(new FlatSplit(connectorId)));
    }
}
