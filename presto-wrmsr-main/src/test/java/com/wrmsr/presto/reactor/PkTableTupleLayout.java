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
package com.wrmsr.presto.reactor;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class PkTableTupleLayout
        extends TableTupleLayout
{
    protected final List<String> pkNames;

    protected final List<String> nkNames;
    protected final List<Integer> pkIndices;
    protected final List<Integer> nkIndices;
    protected final List<Type> pkTypes;
    protected final List<Type> nkTypes;

    protected final TableTupleLayout pk;
    protected final TableTupleLayout nk;

    public PkTableTupleLayout(List<String> names, List<Type> types, List<String> pkNames)
    {
        super(names, types);
        checkArgument(names.size() == types.size());
        this.pkNames = ImmutableList.copyOf(pkNames);

        Set<String> pkNameSet = ImmutableSet.copyOf(pkNames);
        nkNames = names.stream().filter(n -> !pkNameSet.contains(n)).collect(toImmutableList());
        pkIndices = pkNames.stream().map(indices::get).collect(toImmutableList());
        nkIndices = IntStream.range(0, names.size()).boxed().filter(i -> !pkNameSet.contains(names.get(i))).collect(toImmutableList());
        pkTypes = pkIndices.stream().map(types::get).collect(toImmutableList());
        nkTypes = nkIndices.stream().map(types::get).collect(toImmutableList());

        pk = new TableTupleLayout(pkNames, pkTypes);
        nk = new TableTupleLayout(nkNames, nkTypes);
    }

    public PkTableTupleLayout(List<Field> pkFields, List<Field> nkFields)
    {
        this(
                Stream.concat(pkFields.stream(), nkFields.stream()).map(Field::getName).collect(toImmutableList()),
                Stream.concat(pkFields.stream(), nkFields.stream()).map(Field::getType).collect(toImmutableList()),
                pkFields.stream().map(Field::getName).collect(toImmutableList()));

    }

    public List<String> getPkNames()
    {
        return pkNames;
    }

    public List<String> getNkNames()
    {
        return nkNames;
    }

    public List<Integer> getPkIndices()
    {
        return pkIndices;
    }

    public List<Integer> getNkIndices()
    {
        return nkIndices;
    }

    public List<Type> getPkTypes()
    {
        return pkTypes;
    }

    public List<Type> getNkTypes()
    {
        return nkTypes;
    }

    public TableTupleLayout getPk()
    {
        return pk;
    }

    public TableTupleLayout getNk()
    {
        return nk;
    }

    @Override
    public String toString()
    {
        return "PkLayout{" +
                "names=" + names +
                ", types=" + types +
                ", pkNames=" + pkNames +
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
        if (!super.equals(o)) {
            return false;
        }
        PkTableTupleLayout pkLayout = (PkTableTupleLayout) o;
        return Objects.equals(pkNames, pkLayout.pkNames);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), pkNames);
    }
}
