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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.Codecs.Codec;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;

public interface fuuu<K, V>
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

    interface Serial<K, V> extends fuuu<K, V>
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

        default Set<K> containsKeys(Set<? extends K> keys)
        {
            ImmutableSet.Builder<K> builder = ImmutableSet.builder();
            for (K key : keys) {
                if (containsKey(key)) {
                    builder.add(key);
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

    interface Batched<K, V> extends fuuu<K, V>
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

        default boolean containsKey(K key)
        {
            Set<K> keys = containsKeys(ImmutableSet.of(key));
            if (keys.isEmpty()) {
                return false;
            }
            else {
                checkState(keys.contains(key));
                return true;
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

    class Wrapper<K, V> implements fuuu<K, V>
    {
        private final fuuu<K, V> wrapped;

        public Wrapper(fuuu<K, V> wrapped)
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

        public Synchronized(fuuu<K, V> wrapped, Object lock)
        {
            super(wrapped);
            this.lock = lock;
        }

        public Synchronized(fuuu<K, V> wrapped)
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
            V value = map.get(key);
            if (value != null) {
                return Optional.of(value);
            }
            else {
                return Optional.empty();
            }
        }

        @Override
        public boolean containsKey(K key)
        {
            return map.containsKey(key);
        }

        @Override
        public void put(K key, V value)
        {
            map.put(key, value);
        }

        @Override
        public boolean remove(K key)
        {
            return map.remove(key) != null;
        }
    }

    class KeyCodec<KO, KI, V> implements fuuu<KO, V>
    {
        private final fuuu<KI, V> wrapped;
        private final Codec<KO, KI> codec;

        public KeyCodec(fuuu<KI, V> wrapped, Codec<KO, KI> codec)
        {
            this.wrapped = wrapped;
            this.codec = codec;
        }

        @Override
        public Map<KO, V> getAll(Set<? extends KO> keys)
        {
            return wrapped.getAll(
                    keys.stream().map(k -> codec.encode(k)).collect(ImmutableCollectors.toImmutableSet()))
                    .entrySet().stream().map(e -> ImmutablePair.of(codec.decode(e.getKey()), e.getValue())).collect(ImmutableCollectors.toImmutableMap());
        }

        @Override
        public Optional<V> get(KO key)
        {
            return wrapped.get(codec.encode(key));
        }

        @Override
        public void putAll(Map<? extends KO, ? extends V> map)
        {
            wrapped.putAll(map.entrySet().stream().map(e -> ImmutablePair.of(codec.encode(e.getKey()), e.getValue())).collect(ImmutableCollectors.toImmutableMap()));
        }

        @Override
        public void put(KO key, V value)
        {
            wrapped.put(codec.encode(key), value);
        }

        @Override
        public Set<KO> removeAll(Set<? extends KO> keys)
        {
            return wrapped.removeAll(
                    keys.stream().map(k -> codec.encode(k)).collect(ImmutableCollectors.toImmutableSet()))
                    .stream().map(k -> codec.decode(k)).collect(ImmutableCollectors.toImmutableSet());
        }

        @Override
        public boolean remove(KO key)
        {
            return wrapped.remove(codec.encode(key));
        }

        @Override
        public Set<KO> containsKeys(Set<? extends KO> keys)
        {
            return wrapped.containsKeys(keys.stream().map(k -> codec.encode(k)).collect(ImmutableCollectors.toImmutableSet()))
                    .stream().map(k -> codec.decode(k)).collect(ImmutableCollectors.toImmutableSet());
        }

        @Override
        public boolean containsKey(KO key)
        {
            return wrapped.containsKey(codec.encode(key));
        }
    }

    class ValueCodec<K, VO, VI> implements fuuu<K, VO>
    {
        private final fuuu<K, VI> wrapped;
        private final Codec<VO, VI> codec;

        public ValueCodec(fuuu<K, VI> wrapped, Codec<VO, VI> codec)
        {
            this.wrapped = wrapped;
            this.codec = codec;
        }

        @Override
        public Map<K, VO> getAll(Set<? extends K> keys)
        {
            return wrapped.getAll(keys)
                    .entrySet().stream().map(e -> ImmutablePair.of(e.getKey(), codec.decode(e.getValue()))).collect(ImmutableCollectors.toImmutableMap());
        }

        @Override
        public Optional<VO> get(K key)
        {
            return wrapped.get(key).map(v -> codec.decode(v));
        }

        @Override
        public void putAll(Map<? extends K, ? extends VO> map)
        {
            wrapped.putAll(map.entrySet().stream().map(e -> ImmutablePair.of(e.getKey(), codec.encode(e.getValue()))).collect(ImmutableCollectors.toImmutableMap()));
        }

        @Override
        public void put(K key, VO value)
        {
            wrapped.put(key, codec.encode(value));
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
    }
}
