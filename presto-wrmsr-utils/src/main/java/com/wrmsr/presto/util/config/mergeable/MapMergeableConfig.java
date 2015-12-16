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
package com.wrmsr.presto.util.config.mergeable;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.Mergeable;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public abstract class MapMergeableConfig<N extends MapMergeableConfig<N, K, V>, K, V>
    implements MergeableConfig<N>, Iterable<Map.Entry<K, V>>
{
    protected final Map<K, V> entries;

    public MapMergeableConfig()
    {
        this.entries = ImmutableMap.of();
    }

    public MapMergeableConfig(Map<K, V> entries)
    {
        this.entries = ImmutableMap.copyOf(entries);
    }

    public Map<K, V> getEntries()
    {
        return entries;
    }

    @Override
    public String toString()
    {
        return getClass().getName() + "{" +
                "entries=" + entries +
                '}';
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public Mergeable merge(Mergeable other)
    {
        Map mergedMap;
        try {
            mergedMap = ImmutableMap.builder()
                    .putAll(entries)
                    .putAll(((N) other).getEntries())
                    .build();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        mergedMap = newHashMap();
        mergedMap.putAll(entries);
        mergedMap.putAll(((N) other).getEntries());

        N merged;
        try {
            merged = (N) getClass().getConstructor(Map.class).newInstance(mergedMap);
        }
        catch (IllegalAccessException | NoSuchMethodException | InstantiationException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
        return merged;
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator()
    {
        return entries.entrySet().iterator();
    }
}
