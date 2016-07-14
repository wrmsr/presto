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
package com.facebook.presto.sql.rewrite;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.WindowDefinition;
import com.facebook.presto.sql.tree.WindowName;
import com.facebook.presto.sql.tree.WindowSpecification;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

final class WindowRewrite
        implements StatementRewrite.Rewrite
{
    @Override
    public Statement rewrite(Session session, Metadata metadata, SqlParser parser, Optional<QueryExplainer> queryExplainer, Statement node)
    {
        return (Statement) new Visitor().process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        public Visitor()
        {
        }

        @Override
        protected Node visitQuery(Query node, Void context)
        {
            return new Query(
                    node.getWith(),
                    (QueryBody) process(node.getQueryBody(), context),
                    node.getOrderBy(),
                    node.getLimit(),
                    node.getApproximate());
        }

        @Override
        protected Node visitQuerySpecification(QuerySpecification node, Void context)
        {
            Map<String, WindowSpecification> specs = new HashMap<>();
            for (WindowDefinition def : node.getWindow()) {
                checkArgument(!specs.containsKey(def.getName()), "Duplicate window definition '%s'", def.getName());
                WindowSpecification spec = def.getSpecification();
                if (spec.getExistingName().isPresent()) {
                    WindowSpecification existing = specs.get(spec.getExistingName().get());
                    requireNonNull(existing, String.format("Existing window definition '%s' not found for '%s'", spec.getExistingName().get(), def.getName()));
                }
            }
            return node;
        }

        @Override
        protected Node visitWindowName(WindowName node, Void context)
        {
            return node; // new WindowInline(windowSpecifications.get(node.getName()));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }

    private static WindowSpecification resolveWindowReference(WindowSpecification referrer, WindowSpecification referent)
    {
        checkArgument(referrer.getExistingName().isPresent(), "Window specification does not contain reference");
        checkArgument(!referent.getExistingName().isPresent(), "Cannot chain window specification references");

        checkArgument(referrer.getPartitionBy().isEmpty(), "Referrer window specification musn't contain partition clauses");
        List<Expression> partitionBy = referent.getPartitionBy();

        if (referrer.)


    }
}
