package com.wrmsr.presto.function;

import com.google.common.collect.ImmutableList;

import java.util.List;

// String conn, String... command
public class JdbcFunction
    extends StringVarargsFunction
{
    public JdbcFunction()
    {
        super(
                "jdbc",
                "execute raw jdbc statements",
                ImmutableList.of(),
                1,
                "varchar",
                "jdbc",
                ImmutableList.of(Context.class));
    }

    public static class Context
    {

    }
}
