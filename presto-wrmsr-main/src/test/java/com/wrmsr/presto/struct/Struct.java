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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class Struct
        implements Iterable<Field>
{
    private final AliasedName aliasedName;

    private final AliasedNameMap<Field> fieldMap;

    private List<Field> fields;
    private boolean sealed;

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

    public boolean isSealed()
    {
        return sealed;
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
        checkState(!sealed);
        requireNonNull(field);
        fieldMap.put(field.getAliasedName(), field);
        fields = ImmutableList.<Field>builder().addAll(fields).add(field).build();
        return this;
    }

    public Struct seal()
    {
        checkState(!sealed);
        sealed = true;
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
