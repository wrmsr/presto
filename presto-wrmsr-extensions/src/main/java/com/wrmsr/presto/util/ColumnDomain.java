package com.wrmsr.presto.util;

public class ColumnDomain
{
    private final Comparable<?>  min;
    private final Comparable<?> max;

    public ColumnDomain(Comparable<?> min, Comparable<?> max)
    {
        this.min = min;
        this.max = max;
    }

    public Comparable<?> getMin()
    {
        return min;
    }

    public Comparable<?> getMax()
    {
        return max;
    }
}
