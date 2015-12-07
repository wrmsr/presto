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
guideposts (quantile estimation - https://en.wikipedia.org/wiki/Order_statistic_tree ?)

null is a type
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

    public static abstract class Assessor
    {
    }

    public static final class MapAssessor extends Assessor
    {
    }

    public static final class ArrayAssessor extends Assessor
    {
    }

    public static abstract class ScalarAssessor extends Assessor
    {
    }

    public static final class NumberAssessor extends ScalarAssessor
    {
    }

    public static final class NullAssessor extends ScalarAssessor
    {
    }

    public static final class StringAssessor extends ScalarAssessor
    {
    }

    public static final class BooleanAssessor extends ScalarAssessor
    {
    }
}
