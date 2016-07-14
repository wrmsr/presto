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

import com.facebook.presto.sql.parser.SqlParser;
import org.testng.annotations.Test;

public class TestWindowRewrite
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testWindowRewrite()
    {
        new WindowRewrite().rewrite(null, null, )
        SQL_PARSER.createStatement("SELECT * FROM table1 WINDOW a AS ()");
        SQL_PARSER.createStatement("SELECT * FROM table1 WINDOW a AS (PARTITION BY 1)");
        SQL_PARSER.createStatement("SELECT * FROM table1 WINDOW a AS (PARTITION BY 1), b AS (ORDER BY 2 ASC NULLS LAST)");

        // SELECT sum(salary) OVER w, avg(salary) OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);
        // SELECT x, sum(x) OVER w FROM (VALUES (1), (2), (3)) AS t (x) WINDOW w AS (ORDER BY x);
        // SELECT x, sum(x) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM (VALUES (1), (2), (3)) AS t (x) WINDOW w AS (ORDER BY x);
    }
}
