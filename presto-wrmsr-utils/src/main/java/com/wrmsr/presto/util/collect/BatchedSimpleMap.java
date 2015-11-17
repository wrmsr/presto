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
    abstract class Operation<K, V>
    {
        protected final K key;

        public Operation(K key)
        {
            this.key = key;
        }
    }

    final class Get<K, V> extends Operation<K, V>
    {
        public Get(K key)
        {
            super(key);
        }
    }

    final class ContainsKey<K, V> extends Operation<K, V>
    {
        public ContainsKey(K key)
        {
            super(key);
        }
    }

    final class Put<K, V> extends Operation<K, V>
    {
        protected final V value;

        public Put(K key, V value)
        {
            super(key);
            this.value = value;
        }
    }

    final class Remove<K, V> extends Operation<K, V>
    {
        public Remove(K key)
        {
            super(key);
        }
    }

    final class BatchBuilder<K, V>
    {
        private final ImmutableList.Builder<Operation<K, V>> builder;

        public BatchBuilder()
        {
            builder = ImmutableList.builder();
        }

        public BatchBuilder<K, V> get(K key)
        {
            builder.add(new Get<>(key));
            return this;

        }

        public BatchBuilder<K, V> containsKey(K key)
        {
            builder.add(new ContainsKey<>(key));
            return this;
        }

        public BatchBuilder<K, V> put(K key, V value)
        {
            builder.add(new Put<>(key, value));
            return this;
        }

        public BatchBuilder<K, V> remove(K key)
        {
            builder.add(new Remove<>(key));
            return this;
        }

        public List<Operation<K, V>> build()
        {
            return builder.build();
        }
    }

    List<Object> batch(List<Operation<K, V>> operations);

    default Map<K, V> getAll(Set<? extends K> keys)
    {

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
