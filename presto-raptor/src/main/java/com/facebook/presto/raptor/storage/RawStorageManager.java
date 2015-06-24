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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.util.CurrentNodeId;
import com.facebook.presto.raptor.util.PageBuffer;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.min;

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
                config.getMaxBufferSize());
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
            DataSize maxBufferSize)
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
    }

    @Override
    public ConnectorPageSource getPageSource(UUID shardUuid, List<Long> columnIds, List<Type> columnTypes, TupleDomain<RaptorColumnHandle> effectivePredicate)
    {
        return null;
    }

    @Override
    public StoragePageSink createStoragePageSink(List<Long> columnIds, List<Type> columnTypes)
    {
        return null;
    }

    @Override
    public boolean isBackupAvailable()
    {
        return false;
    }

    @Override
    public PageBuffer createPageBuffer()
    {
        return null;
    }
}
