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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MapProxy<K, V> implements Map<K, V>
{
    private final Map<K, V> target;

    public MapProxy(Map<K, V> target)
    {
        this.target = target;
    }

    @Override
    public int size()
    {
        return target.size();
    }

    @Override
    public boolean isEmpty()
    {
        return target.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return target.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        return target.containsValue(value);
    }

    @Override
    public V get(Object key)
    {
        return target.get(key);
    }

    @Override
    public V put(K key, V value)
    {
        return target.put(key, value);
    }

    @Override
    public V remove(Object key)
    {
        return target.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m)
    {
        target.putAll(m);
    }

    @Override
    public void clear()
    {
        target.clear();
    }

    @Override
    public Set<K> keySet()
    {
        return target.keySet();
    }

    @Override
    public Collection<V> values()
    {
        return target.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return target.entrySet();
    }

    @Override
    public boolean equals(Object o)
    {
        return target.equals(o);
    }

    @Override
    public int hashCode()
    {
        return target.hashCode();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue)
    {
        return target.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action)
    {
        target.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function)
    {
        target.replaceAll(function);
    }

    @Override
    public V putIfAbsent(K key, V value)
    {
        return target.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value)
    {
        return target.remove(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue)
    {
        return target.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value)
    {
        return target.replace(key, value);
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction)
    {
        return target.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        return target.computeIfPresent(key, remappingFunction);
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        return target.compute(key, remappingFunction);
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction)
    {
        return target.merge(key, value, remappingFunction);
    }
}
