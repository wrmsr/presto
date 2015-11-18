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
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class PkLayout<N>
        extends Layout<N>
{
    protected final List<N> pkNames;

    protected final List<N> nkNames;
    protected final List<Integer> pkIndices;
    protected final List<Integer> nkIndices;
    protected final List<Type> pkTypes;
    protected final List<Type> nkTypes;

    protected final Layout<N> pk;
    protected final Layout<N> nk;

    public PkLayout(List<N> names, List<Type> types, List<N> pkNames)
    {
        super(names, types);
        checkArgument(names.size() == types.size());
        this.pkNames = ImmutableList.copyOf(pkNames);

        Set<N> pkNameSet = ImmutableSet.copyOf(pkNames);
        nkNames = names.stream().filter(n -> !pkNameSet.contains(n)).collect(toImmutableList());
        pkIndices = pkNames.stream().map(indices::get).collect(toImmutableList());
        nkIndices = IntStream.range(0, names.size()).boxed().filter(i -> !pkNameSet.contains(names.get(i))).collect(toImmutableList());
        pkTypes = pkIndices.stream().map(types::get).collect(toImmutableList());
        nkTypes = nkIndices.stream().map(types::get).collect(toImmutableList());

        pk = new Layout<>(pkNames, pkTypes);
        nk = new Layout<>(nkNames, nkTypes);
    }

    public PkLayout(List<Field<N>> pkFields, List<Field<N>> nkFields)
    {
        this(
                Stream.concat(pkFields.stream(), nkFields.stream()).map(Field::getName).collect(toImmutableList()),
                Stream.concat(pkFields.stream(), nkFields.stream()).map(Field::getType).collect(toImmutableList()),
                pkFields.stream().map(Field::getName).collect(toImmutableList()));
    }

    @Override
    public <T> PkLayout<T> mapNames(Function<N, T> fn)
    {
        return new PkLayout<>(names.stream().map(fn::apply).collect(toImmutableList()), types, pkNames.stream().map(fn::apply).collect(toImmutableList()));
    }

    public List<N> getPkNames()
    {
        return pkNames;
    }

    public List<N> getNkNames()
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

    public Layout<N> getPk()
    {
        return pk;
    }

    public Layout<N> getNk()
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
        PkLayout pkLayout = (PkLayout) o;
        return Objects.equals(pkNames, pkLayout.pkNames);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), pkNames);
    }
}
