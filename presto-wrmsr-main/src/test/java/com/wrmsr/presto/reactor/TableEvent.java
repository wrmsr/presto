package com.wrmsr.presto.reactor;

import java.util.Optional;

public class TableEvent
{
    public enum Operation
    {
        INSERT,
        UPDATE,
        DELETE
    }

    protected final Operation operation;
    protected final Optional<PkTableTuple> before;
    protected final Optional<PkTableTuple> after;

    public TableEvent(Operation operation, Optional<PkTableTuple> before, Optional<PkTableTuple> after)
    {
        this.operation = operation;
        this.before = before;
        this.after = after;
    }

    public Operation getOperation()
    {
        return operation;
    }

    public Optional<PkTableTuple> getBefore()
    {
        return before;
    }

    public Optional<PkTableTuple> getAfter()
    {
        return after;
    }
}
