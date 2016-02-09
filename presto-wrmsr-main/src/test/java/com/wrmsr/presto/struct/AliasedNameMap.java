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
package com.wrmsr.presto.struct;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class AliasedNameMap<V>
        implements Iterable<AliasedName>
{
    private Set<AliasedName> keys;
    private Map<String, V> valuesByLowerCase;

    public AliasedNameMap()
    {
        keys = ImmutableSet.of();
        valuesByLowerCase = ImmutableMap.of();
    }

    public void put(AliasedName key, V value)
    {
        requireNonNull(key);
        requireNonNull(value);
        key.getLowerCase().forEach(n -> checkArgument(!valuesByLowerCase.containsKey(n)));
        key.getLowerCase().forEach(n -> valuesByLowerCase.put(n, value));
    }

    public Optional<V> get(String name)
    {
        return Optional.ofNullable(valuesByLowerCase.get(name.toLowerCase()));
    }

    public boolean containsKey(String name)
    {
        return !get(name).isPresent();
    }

    @Override
    public Iterator<AliasedName> iterator()
    {
        return keys.iterator();
    }

    public Spliterator<AliasedName> spliterator()
    {
        return Spliterators.spliterator(iterator(), keys.size(), Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.SIZED);
    }

    public Stream<AliasedName> stream()
    {
        return StreamSupport.stream(spliterator(), false);
    }
}
