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

import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.wrmsr.presto.raptor.storage.RawStorageManager;
import com.wrmsr.presto.raptor.storage.StorageEngine;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class StorageModule
        implements Module
{
    private final String connectorId;
    private final StorageEngine storageEngine;

    public StorageModule(String connectorId, StorageEngine storageEngine)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.storageEngine = checkNotNull(storageEngine);
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(StorageManagerConfig.class);
        binder.bind(StorageEngine.class).toInstance(storageEngine);
        if (storageEngine == StorageEngine.ORC) {
            binder.bind(StorageManager.class).to(OrcStorageManager.class).in(Scopes.SINGLETON);
        }
        else if (storageEngine == StorageEngine.RAW) {
            binder.bind(StorageManager.class).to(RawStorageManager.class).in(Scopes.SINGLETON);
        }
        else {
            throw new IllegalArgumentException("Unhandled storage engine: " + storageEngine);
        }
        binder.bind(StorageService.class).to(FileStorageService.class).in(Scopes.SINGLETON);
        binder.bind(ShardManager.class).to(DatabaseShardManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardRecoveryManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardCompactionManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardCompactor.class).in(Scopes.SINGLETON);

        newExporter(binder).export(ShardRecoveryManager.class).as(generatedNameOf(ShardRecoveryManager.class, connectorId));
        newExporter(binder).export(StorageManager.class).as(generatedNameOf(OrcStorageManager.class, connectorId));
    }
}
