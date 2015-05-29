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
package com.wrmsr.presto.metaconnectors.util;

import com.google.common.collect.ImmutableMap;

import java.util.function.Function;
import java.util.stream.Collector;

public final class ImmutableCollectors
{
    private ImmutableCollectors() {}

    public static <I, K, V> Collector<I, ImmutableMap.Builder<K, V>, ImmutableMap<K, V>> toImmutableMap(Function<I, K> keyMapper, Function<I, V> valueMapper) {
        return Collector.of(
                ImmutableMap::builder,
                (builder, in) -> builder.put(keyMapper.apply(in), valueMapper.apply(in)),
                (ImmutableMap.Builder<K, V> left, ImmutableMap.Builder<K, V> right) -> left.putAll(right.build()),
                ImmutableMap.Builder<K, V>::build);
    }
}
