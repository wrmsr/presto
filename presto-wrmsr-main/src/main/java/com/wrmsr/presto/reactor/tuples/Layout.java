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
package com.wrmsr.presto.reactor.tuples;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newHashSet;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;

public class Layout<N>
{
    public static class Field<N>
    {
        protected final N name;
        protected final Type type;

        public Field(N name, Type type)
        {
            this.name = name;
            this.type = type;
        }

        public N getName()
        {
            return name;
        }

        public Type getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return "Field{" +
                    "name='" + name + '\'' +
                    ", type=" + type +
                    '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Field field = (Field) o;
            return Objects.equals(name, field.name) &&
                    Objects.equals(type, field.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, type);
        }
    }

    protected final List<N> names;
    protected final List<Type> types;
    protected final Map<N, Integer> indices;

    public Layout(List<N> names, List<Type> types)
    {
        checkArgument(newHashSet(names).size() == names.size());
        checkArgument(names.size() == types.size());
        this.names = ImmutableList.copyOf(names);
        this.types = ImmutableList.copyOf(types);
        indices = IntStream.range(0, names.size()).boxed().map(i -> ImmutablePair.of(names.get(i), i)).collect(toImmutableMap());
    }

    public Layout(List<Field<N>> fields)
    {
        this(fields.stream().map(Field::getName).collect(toImmutableList()), fields.stream().map(Field::getType).collect(toImmutableList()));
    }

    public <T> Layout<T> mapNames(Function<N, T> fn)
    {
        return new Layout<>(names.stream().map(fn::apply).collect(toImmutableList()), types);
    }

    public int size()
    {
        return names.size();
    }

    public boolean isEmpty()
    {
        return names.isEmpty();
    }

    public boolean containsName(N name)
    {
        return indices.containsKey(name);
    }

    public List<N> getNames()
    {
        return names;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public Type getType(N name)
    {
        return types.get(indices.get(name));
    }

    public List<Field<N>> getFields()
    {
        return IntStream.range(0, names.size()).boxed().map(i -> new Field<>(names.get(i), types.get(i))).collect(toImmutableList());
    }

    public Map<N, Integer> getIndices()
    {
        return indices;
    }

    public int get(N name)
    {
        return indices.get(name);
    }

    @Override
    public String toString()
    {
        return "Layout{" +
                "names=" + names +
                ", types=" + types +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Layout layout = (Layout) o;
        return Objects.equals(names, layout.names) &&
                Objects.equals(types, layout.types);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(names, types);
    }
}
