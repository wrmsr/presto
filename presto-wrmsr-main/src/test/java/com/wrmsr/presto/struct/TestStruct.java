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

/*
avro:

null         null    null
boolean      boolean true
int,long     integer 1
float,double number  1.1
bytes        string  "\u00FF"
string       string  "foo"
record       object  {"a": 1}
enum         string  "FOO"
array        array   [1]
map          object  {"a": 1}
fixed        string  "\u00ff"

** derived
*/

/*
swagger | oapi | jsonschema | what the fuck ever:

integer  integer int32     signed 32 bits
long     integer int64     signed 64 bits
float    number  float
double   number  double
string   string
byte     string  byte      base64 encoded characters
binary   string  binary    any sequence of octets
boolean  boolean
date     string  date      As defined by full-date - RFC3339
dateTime string  date-time As defined by date-time - RFC3339
password string  password  Used to hint UIs the input needs to be obscured.
*/

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

    public static class AliasedNameMap<V>
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

    public static class Struct
            implements Iterable<Field>
    {
        private final AliasedName aliasedName;

        private final AliasedNameMap<Field> fieldMap;

        private List<Field> fields;

        public Struct(AliasedName aliasedName)
        {
            this.aliasedName = requireNonNull(aliasedName);

            fieldMap = new AliasedNameMap<>();

            fields = ImmutableList.of();
        }

        public AliasedName getAliasedName()
        {
            return aliasedName;
        }

        public List<Field> getFields()
        {
            return fields;
        }

        public AliasedNameMap<Field> getFieldMap()
        {
            return fieldMap;
        }

        public Optional<Field> getField(String name)
        {
            return fieldMap.get(name);
        }

        public Struct addField(Field field)
        {
            requireNonNull(field);
            field.setStruct(this, fields.size());
            fieldMap.put(field.getAliasedName(), field);
            fields = ImmutableList.<Field>builder().addAll(fields).add(field).build();
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
        private final AliasedName aliasedName;

        private Optional<Struct> struct;
        private OptionalInt position;

        public Field(AliasedName aliasedName)
        {
            this.aliasedName = requireNonNull(aliasedName);

            struct = Optional.empty();
            position = OptionalInt.empty();
        }

        public AliasedName getAliasedName()
        {
            return aliasedName;
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
