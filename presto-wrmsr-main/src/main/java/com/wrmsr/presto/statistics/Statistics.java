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

https://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math3/stat/descriptive/rank/Percentile.html
https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/quantile/QDigest.java
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
