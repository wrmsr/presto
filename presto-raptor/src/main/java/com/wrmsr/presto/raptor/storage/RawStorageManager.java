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
package com.wrmsr.presto.raptor.storage;

import com.facebook.presto.hadoop.shaded.com.google.common.base.Strings;
import com.facebook.presto.hadoop.shaded.com.google.common.base.Throwables;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.storage.Row;
import com.facebook.presto.raptor.storage.ShardRecoveryManager;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.raptor.storage.StorageManagerStats;
import com.facebook.presto.raptor.storage.StoragePageSink;
import com.facebook.presto.raptor.storage.StorageService;
import com.facebook.presto.raptor.util.CurrentNodeId;
import com.facebook.presto.raptor.util.PageBuffer;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import javax.inject.Inject;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.storage.Row.extractRow;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.Math.min;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

public class RawStorageManager
        implements StorageManager
{
    private static final long MAX_ROWS = 1_000_000_000;

    private final String nodeId;
    private final StorageService storageService;
    private final Optional<BackupStore> backupStore;
    private final JsonCodec<ShardDelta> shardDeltaCodec;
    private final ShardRecoveryManager recoveryManager;
    private final Duration recoveryTimeout;
    private final long maxShardRows;
    private final DataSize maxShardSize;
    private final DataSize maxBufferSize;
    private final StorageManagerStats stats;
    private final String compression;
    private final String delimiter;

    @Inject
    public RawStorageManager(
            CurrentNodeId currentNodeId,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            JsonCodec<ShardDelta> shardDeltaCodec,
            StorageManagerConfig config,
            ShardRecoveryManager recoveryManager)
    {
        this(currentNodeId.toString(),
                storageService,
                backupStore,
                shardDeltaCodec,
                recoveryManager,
                config.getShardRecoveryTimeout(),
                config.getMaxShardRows(),
                config.getMaxShardSize(),
                config.getMaxBufferSize(),
                config.getRawCompression(),
                config.getRawDelimiter());
    }

    public RawStorageManager(
            String nodeId,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            JsonCodec<ShardDelta> shardDeltaCodec,
            ShardRecoveryManager recoveryManager,
            Duration shardRecoveryTimeout,
            long maxShardRows,
            DataSize maxShardSize,
            DataSize maxBufferSize,
            String compression,
            String delimiter)
    {
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
        this.storageService = checkNotNull(storageService, "storageService is null");
        this.backupStore = checkNotNull(backupStore, "backupStore is null");
        this.shardDeltaCodec = checkNotNull(shardDeltaCodec, "shardDeltaCodec is null");

        this.recoveryManager = checkNotNull(recoveryManager, "recoveryManager is null");
        this.recoveryTimeout = checkNotNull(shardRecoveryTimeout, "shardRecoveryTimeout is null");

        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = min(maxShardRows, MAX_ROWS);
        this.maxShardSize = checkNotNull(maxShardSize, "maxShardSize is null");
        this.maxBufferSize = checkNotNull(maxBufferSize, "maxBufferSize is null");
        this.stats = new StorageManagerStats();
        this.compression = compression;
        this.delimiter = delimiter;
    }

    @Override
    public ConnectorPageSource getPageSource(UUID shardUuid, List<Long> columnIds, List<Type> columnTypes, TupleDomain<RaptorColumnHandle> effectivePredicate)
    {
        return null;
    }

    @Override
    public StoragePageSink createStoragePageSink(List<Long> columnIds, List<Type> columnTypes)
    {
        checkArgument(columnTypes.size() == 1 && columnTypes.get(0) instanceof VarbinaryType);
        return new RawStoragePageSink(columnIds.get(0), columnTypes.get(0));
    }

    public boolean isBackupAvailable()
    {
        return backupStore.isPresent();
    }

    private void writeShard(UUID shardUuid)
    {
        File stagingFile = storageService.getStagingFile(shardUuid);
        File storageFile = storageService.getStorageFile(shardUuid);

        storageService.createParents(storageFile);

        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to move shard file", e);
        }

        if (isBackupAvailable()) {
            long start = System.nanoTime();
            backupStore.get().backupShard(shardUuid, storageFile);
            stats.addCopyShardDataRate(new DataSize(storageFile.length(), BYTE), nanosSince(start));
        }
    }

    private ShardInfo createShardInfo(UUID shardUuid, File file, Set<String> nodes, long rowCount, Long columnId)
    {
        return new ShardInfo(shardUuid, nodes, ImmutableList.of(), rowCount, file.length(), file.length());
    }

    private class RawStoragePageSink implements StoragePageSink
    {
        private final Long columnId;
        private final Type columnType;

        private final List<ShardInfo> shards = new ArrayList<>();

        private boolean committed;
        private RawFileWriter writer;
        private UUID shardUuid;

        public RawStoragePageSink(Long columnId, Type columnType)
        {
            this.columnId = checkNotNull(columnId, "columnIds is null");
            this.columnType = checkNotNull(columnType, "columnTypes is null");
        }

        @Override
        public void appendPages(List<Page> pages)
        {
            createWriterIfNecessary();
            writer.appendPages(pages);
        }

        @Override
        public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
        {
            createWriterIfNecessary();
            writer.appendPages(inputPages, pageIndexes, positionIndexes);
        }

        @Override
        public void appendRow(Row row)
        {
            createWriterIfNecessary();
            writer.appendRow(row);
        }

        @Override
        public boolean isFull()
        {
            if (writer == null) {
                return false;
            }
            return (writer.getRowCount() >= maxShardRows) || (writer.getSize() >= maxShardSize.toBytes());
        }

        @Override
        public void flush()
        {
            if (writer != null) {
                writer.close();

                File stagingFile = storageService.getStagingFile(shardUuid);

                Set<String> nodes = ImmutableSet.of(nodeId);
                long rowCount = writer.getRowCount();

                shards.add(createShardInfo(shardUuid, stagingFile, nodes, rowCount, columnId));

                writer = null;
                shardUuid = null;
            }
        }

        @Override
        public List<ShardInfo> commit()
        {
            checkState(!committed, "already committed");
            committed = true;

            flush();
            for (ShardInfo shard : shards) {
                writeShard(shard.getShardUuid());
            }
            return ImmutableList.copyOf(shards);
        }

        @Override
        public void rollback()
        {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }

        private void createWriterIfNecessary()
        {
            if (writer == null) {
                shardUuid = UUID.randomUUID();
                File stagingFile = storageService.getStagingFile(shardUuid);
                storageService.createParents(stagingFile);
                writer = new RawFileWriter(columnType, stagingFile);
            }
        }
    }

    private class RawFileWriter
            implements Closeable
    {
        private final Type columnType;

        private final OutputStream writer;

        private long rowCount;
        private long size;

        public RawFileWriter(Type columnType, File target)
        {
            this.columnType = columnType;

            OutputStream writer;

            try {
                writer = new BufferedOutputStream(new FileOutputStream(target));
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, "ioex", e);
            }

            if (!Strings.isNullOrEmpty(compression)) {
                try {
                    writer = new CompressorStreamFactory().createCompressorOutputStream(compression, writer);
                }
                catch (CompressorException e) {
                    throw Throwables.propagate(e);
                }
            }

            this.writer = writer;
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public long getSize()
        {
            return size;
        }

        public void appendPages(List<Page> pages)
        {
            for (Page page : pages) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    appendRow(extractRow(page, position, ImmutableList.of(columnType)));
                }
            }
        }

        public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
        {
            checkArgument(pageIndexes.length == positionIndexes.length, "pageIndexes and positionIndexes do not match");
            for (int i = 0; i < pageIndexes.length; i++) {
                Page page = inputPages.get(pageIndexes[i]);
                appendRow(extractRow(page, positionIndexes[i], ImmutableList.of(columnType)));
            }
        }

        public void appendRow(Row row)
        {
            if (row == null) {
                return;
            }
            List<Object> columns = row.getColumns();
            checkArgument(columns.size() == 1);
            Object value = row.getColumns().get(0);
            checkArgument(value instanceof byte[]);
            byte[] bytes = (byte[]) value;
            try {
                writer.write(bytes, 0, bytes.length);
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, "Failed to write record", e);
            }
            rowCount++;
            size += row.getSizeInBytes();
            if (!Strings.isNullOrEmpty(delimiter)) {
                byte[] delimiterBytes = delimiter.getBytes();
                try {
                    writer.write(delimiterBytes, 0, delimiterBytes.length);
                }
                catch (IOException e) {
                    throw new PrestoException(RAPTOR_ERROR, "Failed to write record", e);
                }
                size += delimiterBytes.length;
            }
        }

        @Override
        public void close()
        {
            try {
                writer.close();
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, "Failed to close writer", e);
            }
        }
    }
}
