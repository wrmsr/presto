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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

public final class ImmutableCollectors
{
    private ImmutableCollectors()
    {
    }

    public static <T> Collector<T, ?, ImmutableList<T>> toImmutableList()
    {
        return Collector.<T, ImmutableList.Builder<T>, ImmutableList<T>>of(
                ImmutableList.Builder::new,
                ImmutableList.Builder::add,
                (left, right) -> {
                    left.addAll(right.build());
                    return left;
                },
                ImmutableList.Builder::build);
    }

    public static <T> Collector<T, ?, ImmutableSet<T>> toImmutableSet()
    {
        return Collector.<T, ImmutableSet.Builder<T>, ImmutableSet<T>>of(
                ImmutableSet.Builder::new,
                ImmutableSet.Builder::add,
                (left, right) -> {
                    left.addAll(right.build());
                    return left;
                },
                ImmutableSet.Builder::build,
                Collector.Characteristics.UNORDERED);
    }

    public static <T> Collector<T, ?, ImmutableMultiset<T>> toImmutableMultiset()
    {
        return Collector.<T, ImmutableMultiset.Builder<T>, ImmutableMultiset<T>>of(
                ImmutableMultiset.Builder::new,
                ImmutableMultiset.Builder::add,
                (left, right) -> {
                    left.addAll(right.build());
                    return left;
                },
                ImmutableMultiset.Builder::build,
                Collector.Characteristics.UNORDERED);
    }

    public static <I, K, V> Collector<I, ImmutableMap.Builder<K, V>, ImmutableMap<K, V>> toImmutableMap(Function<I, K> keyMapper, Function<I, V> valueMapper)
    {
        return Collector.of(
                ImmutableMap::builder,
                (builder, in) -> builder.put(keyMapper.apply(in), valueMapper.apply(in)),
                (ImmutableMap.Builder<K, V> left, ImmutableMap.Builder<K, V> right) -> left.putAll(right.build()),
                ImmutableMap.Builder<K, V>::build);
    }

    public static <K, V> Collector<Map.Entry<K, V>, ImmutableMap.Builder<K, V>, ImmutableMap<K, V>> toImmutableMap()
    {
        return toImmutableMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    public static <I, K, V> Collector<I, ImmutableMultimap.Builder<K, V>, ImmutableMultimap<K, V>> toImmutableMultimap(Function<I, K> keyMapper, Function<I, V> valueMapper)
    {
        return Collector.of(
                ImmutableMultimap::builder,
                (builder, in) -> builder.put(keyMapper.apply(in), valueMapper.apply(in)),
                (ImmutableMultimap.Builder<K, V> left, ImmutableMultimap.Builder<K, V> right) -> left.putAll(right.build()),
                ImmutableMultimap.Builder<K, V>::build);
    }

    public static <K, V> Collector<Map.Entry<K, V>, ImmutableMultimap.Builder<K, V>, ImmutableMultimap<K, V>> toImmutableMultimap()
    {
        return toImmutableMultimap(Map.Entry::getKey, Map.Entry::getValue);
    }
}
