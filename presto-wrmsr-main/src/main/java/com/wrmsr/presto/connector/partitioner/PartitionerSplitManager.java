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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPartition;
import com.facebook.presto.spi.connector.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.collect.ImmutableCollectors;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;

public class PartitionerSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final Connector targetConnector;
    private final ConnectorSplitManager target;
    private final Partitioner partitioner;

    public PartitionerSplitManager(String connectorId, Connector targetConnector, ConnectorSplitManager target, Partitioner partitioner)
    {
        this.connectorId = connectorId;
        this.targetConnector = checkNotNull(targetConnector);
        this.target = checkNotNull(target);
        this.partitioner = checkNotNull(partitioner);
    }

    @Override
    @Deprecated
    public ConnectorPartitionResult getPartitions(ConnectorSession session, ConnectorTableHandle table, TupleDomain<ColumnHandle> tupleDomain)
    {
        ConnectorMetadata metadata = targetConnector.getMetadata();
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, table);
        TupleDomain<String> stringTupleDomain = TupleDomain.all();
        for (Map.Entry<ColumnHandle, Domain> e : tupleDomain.getDomains().get().entrySet()) {
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, table, e.getKey());
            stringTupleDomain = TupleDomain.columnWiseUnion(
                    stringTupleDomain,
                    TupleDomain.withColumnDomains(ImmutableMap.of(columnMetadata.getName(), e.getValue())));
        }
        List<Partitioner.Partition> partitions = partitioner.getPartitionsConnector(tableMetadata.getTable(), stringTupleDomain);

        List<ConnectorPartition> ret = newArrayList();
        TupleDomain<ColumnHandle> undeterminedTupleDomain = TupleDomain.none();
        for (Partitioner.Partition partition : partitions) {
            TupleDomain intersectedDomain = tupleDomain.intersect(
                    TupleDomain.withColumnDomains(
                            partition.getTupleDomain().getDomains().get().entrySet().stream().collect(ImmutableCollectors.toImmutableMap(
                                    e -> metadata.getColumnHandles(session, table).get(e.getKey()), e -> e.getValue()))));

            if (intersectedDomain.isNone()) {
                continue;
            }

            ConnectorPartitionResult r = target.getPartitions(session, table, intersectedDomain);
            ret.addAll(r.getPartitions()); // FIXME PARTITION-IDS

            // if (r.getUndeterminedTupleDomain()) // FIXME
            undeterminedTupleDomain = TupleDomain.columnWiseUnion(
                    undeterminedTupleDomain,
                    r.getUndeterminedTupleDomain().intersect(intersectedDomain));
        }

        return new ConnectorPartitionResult(Collections.unmodifiableList(ret), undeterminedTupleDomain);
    }

    @Override
    @Deprecated
    public ConnectorSplitSource getPartitionSplits(ConnectorSession session, ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        List<ConnectorSplit> splits = partitions.stream()
                .map(p -> target.getPartitionSplits(session, table, ImmutableList.of(p)))
                .flatMap(s -> {
                    try {
                        return s.getNextBatch(Integer.MAX_VALUE).get().stream();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(ImmutableCollectors.toImmutableList());
        return new FixedSplitSource(connectorId, splits);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        return target.getSplits(session, layout);
    }

    /*
    @Override
    public ConnectorSplitSource getSplits(ConnectorTableLayoutHandle layout)
    {
        TpchTableHandle tableHandle = checkType(layout, TpchTableLayoutHandle.class, "layout").getTable();

        Set<Node> nodes = nodeManager.getActiveDatasourceNodes(connectorId);
        checkState(!nodes.isEmpty(), "No TPCH nodes available");

        int totalParts = nodes.size() * splitsPerNode;
        int partNumber = 0;

        // Split the data using split and skew by the number of nodes available.
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (Node node : nodes) {
            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(new TpchSplit(tableHandle, partNumber, totalParts, ImmutableList.of(node.getHostAndPort())));
                partNumber++;
            }
        }
        return new FixedSplitSource(connectorId, splits.build());
    }
    */
}
