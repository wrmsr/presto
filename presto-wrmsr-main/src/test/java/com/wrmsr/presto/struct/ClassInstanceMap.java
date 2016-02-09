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

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ClassInstanceMap<T>
        implements Iterable<T>
{
    private final Class<? extends T> cls;

    private Map<Class<? extends T>, T> instancesByClass;

    public ClassInstanceMap(Class<? extends T> cls)
    {
        this.cls = requireNonNull(cls);
        instancesByClass = ImmutableMap.of();
    }

    public Class<? extends T> getInstanceClass()
    {
        return cls;
    }

    public void put(T instance)
    {
        requireNonNull(instance);
        checkArgument(!instancesByClass.containsKey(instance.getClass()));
        instancesByClass.put((Class<? extends T>) instance.getClass(), instance);
    }

    public <C extends T> Optional<T> get(Class<? extends C> cls)
    {
        return Optional.ofNullable(cls.cast(instancesByClass.get(cls)));
    }

    public boolean containsKey(Class<? extends T> cls)
    {
        return !get(cls).isPresent();
    }

    @Override
    public Iterator<T> iterator()
    {
        return instancesByClass.values().iterator();
    }

    public Spliterator<T> spliterator()
    {
        return Spliterators.spliterator(iterator(), instancesByClass.size(), Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.SIZED);
    }

    public Stream<T> stream()
    {
        return StreamSupport.stream(spliterator(), false);
    }
}
