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
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.WindowName;
import com.facebook.presto.sql.tree.WindowInline;
import com.facebook.presto.sql.tree.WindowSpecification;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class WindowRewrite
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
        private final Map<String, WindowSpecification> windowSpecifications = new HashMap<>();

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
            node.getWindow().forEach(wd -> windowSpecifications.put(wd.getName(), wd.getSpecification()));
            return node;
        }

        @Override
        protected Node visitWindowName(WindowName node, Void context)
        {
            return new WindowInline(windowSpecifications.get(node.getName()));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
