package com.wrmsr.presto.util;

import java.util.List;
import java.util.stream.IntStream;

import static com.wrmsr.presto.util.ImmutableCollectors.toImmutableList;

public class Lists
{
    private Lists()
    {
    }

    public static <E> List<E> listOf(int size, E value)
    {
        return IntStream.range(0, size).boxed().map(i -> value).collect(toImmutableList());
    }
}
