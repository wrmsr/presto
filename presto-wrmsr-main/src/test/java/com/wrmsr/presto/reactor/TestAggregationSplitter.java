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
package com.wrmsr.presto.reactor;

/*
*** reversible ComebineFunctions
 - just ghetto wholesale rebuild till then lols

KVConnector?
 - or all kvs expressible in terms of connectors

strats:
 - two maps:
  pk -> jk
  jk -> [pk]



*/

import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import org.testng.annotations.Test;

public class TestAggregationSplitter
{
    public final TestHelper helper = new TestHelper();

    public static class AggregationSplitter extends PlanVisitor<AggregationSplitter.Context, PlanNode>
    {
        public static class Context
        {

        }
    }

    @Test
    public void testSplitter() throws Throwable
    {
        TestHelper.PlannedQuery pq = helper.plan("select nationkey, count(*) c from tpch.tiny.nation group by nationkey");
        System.out.println(pq);
    }
}
