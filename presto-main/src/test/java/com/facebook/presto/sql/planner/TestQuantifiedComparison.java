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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestQuantifiedComparison
        extends BasePlanTest
{
    @Test
    public void testQuantifiedComparisonEqualsAny()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey = ANY (VALUES ROW(CAST(5 as BIGINT)), ROW(CAST(3 as BIGINT)))";
        assertPlan(query, anyTree(
                filter("S",
                        project(
                                semiJoin("X", "Y", "S",
                                        anyTree(tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                        anyTree(values(ImmutableMap.of("Y", 0))))))));
    }

    @Test
    public void testQuantifiedComparisonNotEqualsAll()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey <> ALL (VALUES ROW(CAST(5 as BIGINT)), ROW(CAST(3 as BIGINT)))";
        assertPlan(query, anyTree(
                filter("NOT S",
                        project(
                                semiJoin("X", "Y", "S",
                                        anyTree(tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                        anyTree(values(ImmutableMap.of("Y", 0))))))));
    }

    @Test
    public void testQuantifiedComparisonLessAll()
    {
        assertOrderedQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey < ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))",
                "X < MIN", "X", "min", "MIN");
    }

    @Test
    public void testQuantifiedComparisonGreaterEqualAll()
    {
        assertOrderedQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey >= ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))",
                "X >= MAX", "X", "max", "MAX");
    }

    @Test
    public void testQuantifiedComparisonLessSome()
    {
        assertOrderedQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey < SOME (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))",
                "X < MAX", "X", "max", "MAX");
    }

    @Test
    public void testQuantifiedComparisonGreaterEqualAny()
    {
        assertOrderedQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey >= ANY (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))",
                "X >= MIN", "X", "min", "MIN");
    }

    @Test
    public void testQuantifiedComparisonEqualAll()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey = ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))";

        assertPlan(query,
                anyTree(
                node(JoinNode.class,
                        anyTree(
                                tableScan("orders")),
                        anyTree(
                                node(AggregationNode.class,
                                        node(ValuesNode.class))))));
    }

    @Test
    public void testQuantifiedComparisonNotEqualAny()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey <> SOME (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))";

        assertPlan(query, anyTree(
                node(JoinNode.class,
                        tableScan("orders"),
                        node(AggregationNode.class,
                                node(ValuesNode.class)))));
    }

    private void assertOrderedQuantifiedComparison(String query, String filter, String columnMapping, String function, String functionAlias)
    {
        assertPlan(query, anyTree(
                project(
                        join(INNER, ImmutableList.of(), Optional.of(filter),
                                tableScan("orders", ImmutableMap.of(columnMapping, "orderkey")),
                                aggregation(
                                        ImmutableMap.of(
                                                functionAlias, functionCall(function, ImmutableList.of("FIELD"))),
                                        values(ImmutableMap.of("FIELD", 0)))))));
    }
}
