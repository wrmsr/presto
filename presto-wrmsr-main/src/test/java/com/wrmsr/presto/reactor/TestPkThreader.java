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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

public class TestPkThreader
{
    public final TestHelper helper = new TestHelper();

    public static class PkThreader extends SimplePlanRewriter<PkThreader.Context>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Map<String, ConnectorSupport> connectorSupport;

        public PkThreader(PlanNodeIdAllocator idAllocator, Map<String, ConnectorSupport> connectorSupport)
        {
            this.idAllocator = idAllocator;
            this.connectorSupport = connectorSupport;
        }

        public static class Context
        {

        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context)
        {
            ConnectorSupport cs = connectorSupport.get(node.getTable().getConnectorId());
            List<String> pks = cs.getPrimaryKey(cs.getSchemaTableName(node.getTable().getConnectorHandle()))
            return super.visitTableScan(node, context);
        }
    }

    @Test
    public void testThing()
            throws Throwable
    {
        TestHelper.PlannedQuery pq = helper.plan("select name from tpch.tiny.customer");
        PkThreader.Context ctx = new PkThreader.Context();
        PkThreader r = new PkThreader(
                pq.idAllocator,
                pq.connectorSupport
        );
        SimplePlanRewriter.rewriteWith(r, pq.plan.getRoot(), ctx);
    }
}
