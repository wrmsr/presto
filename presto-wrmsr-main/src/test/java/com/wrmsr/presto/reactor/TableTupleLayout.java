package com.wrmsr.presto.reactor;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;

public class TableTupleLayout
{
    protected final List<String> names;
    protected final List<Type> types;
    protected final Map<String, Integer> indices;

    public TableTupleLayout(List<String> names, List<Type> types)
    {
        checkArgument(names.size() == types.size());
        this.names = ImmutableList.copyOf(names);
        this.types = ImmutableList.copyOf(types);
        indices = IntStream.range(0, names.size()).boxed().map(i -> ImmutablePair.of(names.get(i), i)).collect(toImmutableMap());
    }

    public List<String> getNames()
    {
        return names;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public Map<String, Integer> getIndices()
    {
        return indices;
    }

    public int get(String name)
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
        TableTupleLayout layout = (TableTupleLayout) o;
        return Objects.equals(names, layout.names) &&
                Objects.equals(types, layout.types);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(names, types);
    }
}
