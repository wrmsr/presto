package com.wrmsr.presto.statistics;

/*
type inference
structure inference
size distribution
null/absence
precision
min/max *
mean/median/mode
cardinality estimation
guideposts
*/

public class Statistics
{
    public static abstract class Statistic
    {
    }

    public static final class Size extends Statistic
    {
    }

    public static final class Bounds extends Statistic
    {
    }
}
