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
package com.wrmsr.presto;

import com.facebook.presto.cassandra.CassandraColumnHandle;
import com.facebook.presto.cassandra.CassandraConnectorFactory;
import com.facebook.presto.cassandra.CassandraConnectorRecordSinkProvider;
import com.facebook.presto.cassandra.CassandraHandleResolver;
import com.facebook.presto.cassandra.CassandraMetadata;
import com.facebook.presto.cassandra.CassandraRecordSetProvider;
import com.facebook.presto.cassandra.CassandraSplit;
import com.facebook.presto.cassandra.CassandraSplitManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.cassandra.CassandraTestingUtils.HOSTNAME;
import static com.facebook.presto.cassandra.CassandraTestingUtils.KEYSPACE_NAME;
import static com.facebook.presto.cassandra.CassandraTestingUtils.PORT;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_NAME;
import static com.facebook.presto.cassandra.CassandraTestingUtils.initializeTestData;
import static com.facebook.presto.cassandra.util.Types.checkType;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestConnectorMap
{
    protected static final String INVALID_DATABASE = "totally_invalid_database";
    private static final Date DATE = new Date();
    protected String database;
    protected SchemaTableName table;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName invalidTable;
    private Connector connector;
    private ConnectorMetadata metadata;
    private ConnectorSplitManager splitManager;
    private ConnectorRecordSetProvider recordSetProvider;
    private ConnectorRecordSinkProvider recordSinkProvider;

    @BeforeClass
    public void setup()
            throws Exception
    {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();

        initializeTestData(DATE);

        String connectorId = "cassandra-test";
        CassandraConnectorFactory connectorFactory = new CassandraConnectorFactory(
                connectorId,
                ImmutableMap.<String, String>of());

        connector = connectorFactory.create(connectorId, ImmutableMap.of(
                "cassandra.contact-points", HOSTNAME,
                "cassandra.native-protocol-port", Integer.toString(PORT)));

        ConnectorTransactionHandle cth = connector.beginTransaction(IsolationLevel.READ_COMMITTED, false);
        metadata = connector.getMetadata(cth);
        assertInstanceOf(metadata, CassandraMetadata.class);
        connector.rollback(cth);

        splitManager = connector.getSplitManager();
        assertInstanceOf(splitManager, CassandraSplitManager.class);

        recordSetProvider = connector.getRecordSetProvider();
        assertInstanceOf(recordSetProvider, CassandraRecordSetProvider.class);

        recordSinkProvider = connector.getRecordSinkProvider();
        assertInstanceOf(recordSinkProvider, CassandraConnectorRecordSinkProvider.class);

//        ConnectorHandleResolver handleResolver = connector.getHandleResolver();
//        assertInstanceOf(handleResolver, CassandraHandleResolver.class);

        database = KEYSPACE_NAME.toLowerCase();
        table = new SchemaTableName(database, TABLE_NAME.toLowerCase());
        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        invalidTable = new SchemaTableName(database, "totally_invalid_table_name");
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
    }

    @Test
    private void testGetRow()
            throws Exception
    {
        ConnectorTransactionHandle cth = connector.beginTransaction(IsolationLevel.READ_COMMITTED, false);

        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(SESSION, tableHandle);
        RecordSink recordSink = recordSinkProvider.getRecordSink(cth, SESSION, insertTableHandle);
        recordSink.beginRecord(1);
        recordSink.appendString("key 33".getBytes());
        for (int i = 0; i < 4; ++i) {
            recordSink.appendNull();
        }
        recordSink.finishRecord();
        recordSink.commit();

        List<String> targetKeys = ImmutableList.of("key 3", "key 9", "key 33");

        TupleDomain<ColumnHandle> td = TupleDomain.withColumnDomains(ImmutableMap.of(
                columnHandles.get(columnIndex.get("key")),
                Domain.create(ValueSet.none(VARCHAR).union(
                        targetKeys.stream()
                                .map(k -> ValueSet.of(VARCHAR, Slices.wrappedBuffer(k.getBytes())))
                                .collect(toImmutableList())), false)));

//        TupleDomain<ColumnHandle> td = TupleDomain.fromFixedValues(
//                ImmutableMap.of(
//                        columnHandles.get(columnIndex.get("key")),
//                        NullableValue.of(VARCHAR, Slices.wrappedBuffer(targetKey.getBytes()))));

        ConnectorPartitionResult partitionResult = splitManager.getPartitions(cth, SESSION, tableHandle, td);
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(cth, SESSION, tableHandle, partitionResult.getPartitions()));

        for (ConnectorSplit split : splits) {
            CassandraSplit cassandraSplit = (CassandraSplit) split;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(cth, SESSION, cassandraSplit, columnHandles).cursor()) {
                while (cursor.advanceNextPosition()) {
                    String keyValue = cursor.getSlice(columnIndex.get("key")).toStringUtf8();
                    assertTrue(keyValue.startsWith("key "));
                    System.out.println(keyValue);
                }
            }
        }

        connector.rollback(cth);
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(1000)));
        }
        return splits.build();
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            String name = checkType(columnHandle, CassandraColumnHandle.class, "columnHandle").getName();
            index.put(name, i);
            i++;
        }
        return index.build();
    }
}
