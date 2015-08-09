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
package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.units.Duration;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachingPartitioner implements Partitioner
{
    private final class Key
    {
        private final SchemaTableName table;
        private final TupleDomain<String> tupleDomain;

        public Key(SchemaTableName table, TupleDomain<String> tupleDomain)
        {
            this.table = table;
            this.tupleDomain = tupleDomain;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Key key = (Key) o;

            if (table != null ? !table.equals(key.table) : key.table != null) {
                return false;
            }
            return !(tupleDomain != null ? !tupleDomain.equals(key.tupleDomain) : key.tupleDomain != null);

        }

        @Override
        public int hashCode()
        {
            int result = table != null ? table.hashCode() : 0;
            result = 31 * result + (tupleDomain != null ? tupleDomain.hashCode() : 0);
            return result;
        }
    }

    private final LoadingCache<Key, List<Partition>> cache;

    public CachingPartitioner(Partitioner target, ExecutorService executor, Duration cacheTtl, Duration refreshInterval )
    {
        long expiresAfterWriteMillis = checkNotNull(cacheTtl, "cacheTtl is null").toMillis();
        long refreshMills = checkNotNull(refreshInterval, "refreshInterval is null").toMillis();
        this.cache = CacheBuilder.newBuilder()
            .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
            .refreshAfterWrite(refreshMills, MILLISECONDS)
            .build(asyncReloading(new CacheLoader<Key, List<Partition>>()
            {
                @Override
                public List<Partition> load(Key key)
                        throws Exception
                {
                    return target.getPartitionsConnector(key.table, key.tupleDomain);
                }
            }, executor));
    }

    @Override
    public List<Partition> getPartitionsConnector(SchemaTableName table, TupleDomain<String> tupleDomain)
    {
        return cache.getUnchecked(new Key(table, tupleDomain));
    }
}
