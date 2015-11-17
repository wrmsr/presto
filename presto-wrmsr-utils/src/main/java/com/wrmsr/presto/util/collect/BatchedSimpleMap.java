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
package com.wrmsr.presto.util.collect;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BatchedSimpleMap<K, V> extends SimpleMap<K, V>
{
    abstract class BatchOperation<K, V>
    {
        protected final K key;

        public BatchOperation(K key)
        {
            this.key = key;
        }
    }

    final class GetBatchOperation<K, V> extends BatchOperation<K, V>
    {
        public GetBatchOperation(K key)
        {
            super(key);
        }
    }

    final class ContainsKeyBatchOperation<K, V> extends BatchOperation<K, V>
    {
        public ContainsKeyBatchOperation(K key)
        {
            super(key);
        }
    }

    final class PutBatchOperation<K, V> extends BatchOperation<K, V>
    {
        protected final V value;

        public PutBatchOperation(K key, V value)
        {
            super(key);
            this.value = value;
        }
    }

    final class RemoveBatchOperation<K, V> extends BatchOperation<K, V>
    {
        public RemoveBatchOperation(K key)
        {
            super(key);
        }
    }

    abstract class BatchResult<K, V>
    {
        protected final K key;
        protected final V value;

        public BatchResult(K key, V value)
        {
            this.key = key;
            this.value = value;
        }

        public K getKey()
        {
            return key;
        }

        public V getValue()
        {
            return value;
        }

        public boolean isPresent()
        {
            return value != null;
        }
    }

    final class GetBatchResult<K, V> extends BatchResult<K, V>
    {
        public GetBatchResult(K key, V value)
        {
            super(key, value);
        }
    }

    final class ContainsKeyBatchResult<K, V> extends BatchResult<K, V>
    {
        protected final boolean isPresent;

        public ContainsKeyBatchResult(K key, boolean isPresent)
        {
            super(key, null);
            this.isPresent = isPresent;
        }

        @Override
        public V getValue()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isPresent()
        {
            return isPresent;
        }
    }

    final class PutBatchResult<K, V> extends BatchResult<K, V>
    {
        public PutBatchResult(K key)
        {
            super(key, null);
        }
    }

    final class RemoveBatchResult<K, V> extends BatchResult<K, V>
    {
        public RemoveBatchResult(K key)
        {
            super(key, null);
        }
    }

    final class BatchBuilder<K, V>
    {
        private final ImmutableList.Builder<BatchOperation<K, V>> builder;

        public BatchBuilder()
        {
            builder = ImmutableList.builder();
        }

        public BatchBuilder<K, V> get(K key)
        {
            builder.add(new GetBatchOperation<>(key));
            return this;

        }

        public BatchBuilder<K, V> containsKey(K key)
        {
            builder.add(new ContainsKeyBatchOperation<>(key));
            return this;
        }

        public BatchBuilder<K, V> put(K key, V value)
        {
            builder.add(new PutBatchOperation<>(key, value));
            return this;
        }

        public BatchBuilder<K, V> remove(K key)
        {
            builder.add(new RemoveBatchOperation<>(key));
            return this;
        }

        public List<BatchOperation<K, V>> build()
        {
            return builder.build();
        }
    }

    static <K, V> BatchBuilder<K, V> batchBuilder()
    {
        return new BatchBuilder<>();
    }

    List<BatchResult<K, V>> batch(List<BatchOperation<K, V>> operations);

    default <K, V> getAll(List<? extends K> keys)
    {
        BatchBuilder<K, V> batchBuilder = batchBuilder();
        keys.forEach();

    }

    default Set<K> containsKeys(Set<? extends K> keys)
    {

    }

    default void putAll(Map<? extends K, ? extends V> map)
    {

    }

    default void removeAll(Set<? extends K> keys)
    {

    }
}
