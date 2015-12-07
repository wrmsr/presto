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

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

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
        public abstract void accept(Object object);
    }

    public static final class TypeSwitchingAssessor extends Assessor
    {
        private final Map<Class<? extends Assessor>, Assessor> children = newHashMap();

        @Override
        public void accept(Object object)
        {
        }
    }

    public static final class MapAssessor extends Assessor
    {
        @Override
        public void accept(Object object)
        {
        }
    }

    public static final class ArrayAssessor extends Assessor
    {
        @Override
        public void accept(Object object)
        {
        }
    }

    public static abstract class ScalarAssessor extends Assessor
    {
        @Override
        public void accept(Object object)
        {
        }
    }

    public static final class NumberAssessor extends ScalarAssessor
    {
        @Override
        public void accept(Object object)
        {
        }
    }

    public static final class NullAssessor extends ScalarAssessor
    {
        @Override
        public void accept(Object object)
        {
        }
    }

    public static final class StringAssessor extends ScalarAssessor
    {
        @Override
        public void accept(Object object)
        {
        }
    }

    public static final class BooleanAssessor extends ScalarAssessor
    {
        @Override
        public void accept(Object object)
        {
        }
    }
}
