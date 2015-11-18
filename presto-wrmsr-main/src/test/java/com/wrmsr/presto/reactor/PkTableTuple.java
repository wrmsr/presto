package com.wrmsr.presto.reactor;

import java.util.List;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class PkTableTuple
        extends TableTuple
{
    public PkTableTuple(PkTableTupleLayout layout, List<Object> values)
    {
        super(layout, values);
    }

    public PkTableTupleLayout getPkLayout()
    {
        return (PkTableTupleLayout) layout;
    }

    public List<Object> getPkValues()
    {
        return getPkLayout().getPkIndices().stream().map(values::get).collect(toImmutableList());
    }

    public List<Object> getNkValues()
    {
        return getPkLayout().getNkIndices().stream().map(values::get).collect(toImmutableList());
    }

    public TableTuple getPk()
    {
        return new TableTuple(getPkLayout().getPk(), getPkValues());
    }

    public TableTuple getNk()
    {
        return new TableTuple(getPkLayout().getNk(), getNkValues());
    }
}
