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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class TestStruct
{
    public static class AliasedName
            implements Iterable<String>
    {
        private final String name;
        private final Set<String> aliases;

        private final Set<String> lowerCase;

        public AliasedName(String name, Set<String> aliases)
        {
            this.name = requireNonNull(name);
            this.aliases = ImmutableSet.copyOf(aliases);

            lowerCase = ImmutableSet.<String>builder()
                    .add(name.toLowerCase())
                    .addAll(aliases.stream().map(String::toLowerCase).collect(toImmutableSet()))
                    .build();
            checkState(lowerCase.size() == aliases.size() + 1);
        }

        public AliasedName(String name, String... aliases)
        {
            this(name, ImmutableSet.copyOf(Arrays.asList(aliases)));
        }

        public String getName()
        {
            return name;
        }

        public Set<String> getAliases()
        {
            return aliases;
        }

        public Set<String> getLowerCase()
        {
            return lowerCase;
        }

        public boolean contains(String name)
        {
            return lowerCase.contains(name);
        }

        @Override
        public Iterator<String> iterator()
        {
            return Iterators.concat(Iterators.singletonIterator(name), aliases.iterator());
        }

        public Spliterator<String> spliterator()
        {
            return Spliterators.spliterator(iterator(), 1 + aliases.size(), Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.SIZED);
        }

        public Stream<String> stream()
        {
            return StreamSupport.stream(spliterator(), false);
        }
    }

    public static class Struct
            implements Iterable<Field>
    {
        private final AliasedName aliasedName;

        private List<Field> fields;
        private Map<String, Field> fieldsByLowerCase;

        public Struct(AliasedName aliasedName)
        {
            this.aliasedName = requireNonNull(aliasedName);

            fields = ImmutableList.of();
            fieldsByLowerCase = ImmutableMap.of();
        }

        public AliasedName getAliasedName()
        {
            return aliasedName;
        }

        public List<Field> getFields()
        {
            return fields;
        }

        public Optional<Field> getField(String name)
        {
            return Optional.ofNullable(fieldsByLowerCase.get(name.toLowerCase()));
        }

        public Struct addField(Field field)
        {
            requireNonNull(field);
            field.getName().getLowerCase().forEach(n -> checkArgument(!fieldsByLowerCase.containsKey(n)));
            return this;
        }

        @Override
        public Iterator<Field> iterator()
        {
            return fields.iterator();
        }

        public Spliterator<Field> spliterator()
        {
            return Spliterators.spliterator(iterator(), fields.size(), Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.SIZED);
        }

        public Stream<Field> stream()
        {
            return StreamSupport.stream(spliterator(), false);
        }
    }

    public static abstract class Field
    {
        private final AliasedName name;

        private Optional<Struct> struct;
        private OptionalInt position;

        public Field(AliasedName name)
        {
            this.name = requireNonNull(name);

            struct = Optional.empty();
            position = OptionalInt.empty();
        }

        public AliasedName getName()
        {
            return name;
        }

        public int getPosition()
        {
            return position.getAsInt();
        }

        public Struct getStruct()
        {
            return struct.get();
        }

        public void setStruct(Struct struct, int position)
        {
            requireNonNull(struct);
            checkArgument(position >= 0);
            checkState(!this.struct.isPresent());
            checkState(!this.position.isPresent());

            this.struct = Optional.of(struct);
            this.position = OptionalInt.of(position);
        }
    }

    @Test
    public void test()
            throws Throwable
    {
    }
}
