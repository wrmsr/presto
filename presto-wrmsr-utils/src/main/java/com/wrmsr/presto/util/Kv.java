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
package com.wrmsr.presto.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public interface Kv<K, V>
{
    Map<K, V> getAll(Set<? extends K> keys);

    Optional<V> get(K key);

    default Set<K> containsKeys(Set<? extends K> keys)
    {
        return getAll(keys).keySet();
    }

    default boolean containsKey(K key)
    {
        return get(key).isPresent();
    }

    void putAll(Map<? extends K, ? extends V> map);

    void put(K key, V value);

    Set<K> removeAll(Set<? extends K> keys);

    boolean remove(K key);

    interface Serial<K, V> extends Kv<K, V>
    {
        default Map<K, V> getAll(Set<? extends K> keys)
        {
            ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
            for (K key : keys) {
                Optional<V> value = get(key);
                if (value.isPresent()) {
                    builder.put(key, value.get());
                }
            }
            return builder.build();
        }

        default void putAll(Map<? extends K, ? extends V> map)
        {
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                put(entry.getKey(), entry.getValue());
            }
        }

        default Set<K> removeAll(Set<? extends K> keys)
        {
            ImmutableSet.Builder<K> builder = ImmutableSet.builder();
            for (K key : keys) {
                if (remove(key)) {
                    builder.add(key);
                }
            }
            return builder.build();
        }
    }

    interface Batched<K, V> extends Kv<K, V>
    {
        default Optional<V> get(K key)
        {
            Map<K, V> map = getAll(ImmutableSet.of(key));
            if (map.isEmpty()) {
                return Optional.empty();
            }
            else {
                checkState(map.containsKey(key));
                return Optional.of(map.get(key));
            }
        }

        default void put(K key, V value)
        {
            putAll(ImmutableMap.of(key, value));
        }

        default boolean remove(K key)
        {
            Set<K> keys = removeAll(ImmutableSet.of(key));
            if (keys.isEmpty()) {
                return false;
            }
            else {
                checkState(keys.contains(key));
                return true;
            }
        }
    }

    class Wrapper<K, V> implements Kv<K, V>
    {
        private final Kv<K, V> wrapped;

        public Wrapper(Kv<K, V> wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public Map<K, V> getAll(Set<? extends K> keys)
        {
            return wrapped.getAll(keys);
        }

        @Override
        public Optional<V> get(K key)
        {
            return wrapped.get(key);
        }

        @Override
        public Set<K> containsKeys(Set<? extends K> keys)
        {
            return wrapped.containsKeys(keys);
        }

        @Override
        public boolean containsKey(K key)
        {
            return wrapped.containsKey(key);
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> map)
        {
            wrapped.putAll(map);
        }

        @Override
        public void put(K key, V value)
        {
            wrapped.put(key, value);
        }

        @Override
        public Set<K> removeAll(Set<? extends K> keys)
        {
            return wrapped.removeAll(keys);
        }

        @Override
        public boolean remove(K key)
        {
            return wrapped.remove(key);
        }
    }

    class Synchronized<K, V> extends Wrapper<K, V>
    {
        private final Object lock;

        public Synchronized(Kv<K, V> wrapped, Object lock)
        {
            super(wrapped);
            this.lock = lock;
        }

        public Synchronized(Kv<K, V> wrapped)
        {
            this(wrapped, wrapped);
        }

        @Override
        public Map<K, V> getAll(Set<? extends K> keys)
        {
            synchronized (lock) {
                return super.getAll(keys);
            }
        }

        @Override
        public Optional<V> get(K key)
        {
            synchronized (lock) {
                return super.get(key);
            }
        }

        @Override
        public Set<K> containsKeys(Set<? extends K> keys)
        {
            synchronized (lock) {
                return super.containsKeys(keys);
            }
        }

        @Override
        public boolean containsKey(K key)
        {
            synchronized (lock) {
                return super.containsKey(key);
            }
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> map)
        {
            synchronized (lock) {
                super.putAll(map);
            }
        }

        @Override
        public void put(K key, V value)
        {
            synchronized (lock) {
                super.put(key, value);
            }
        }

        @Override
        public Set<K> removeAll(Set<? extends K> keys)
        {
            synchronized (lock) {
                return super.removeAll(keys);
            }
        }

        @Override
        public boolean remove(K key)
        {
            synchronized (lock) {
                return super.remove(key);
            }
        }
    }

    class MapImpl<K, V> implements Serial<K, V>
    {
        private final Map<K, V> map;

        public MapImpl(Map<K, V> map)
        {
            this.map = map;
        }

        @Override
        public Optional<V> get(K key)
        {
            if (map.containsKey(key)) {
                return Optional.of(map.get(key));
            }
            else {
                return Optional.empty();
            }
        }

        @Override
        public void put(K key, V value)
        {
            map.put(key, value);
        }

        @Override
        public boolean remove(K key)
        {
            if (map.containsKey(key)) {
                map.remove(key);
                return true;
            }
            else {
                return false;
            }
        }
    }
}
