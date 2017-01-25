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

package com.facebook.presto.tests.hive;

import com.teradata.tempto.BeforeTestWithContext;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.AUTHORIZATION;
import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.utils.QueryExecutors.connectToPresto;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static java.lang.String.format;

public class TestGrantRevoke
    extends ProductTest
{
    private String tableName;
    private QueryExecutor aliceExecutor;
    private QueryExecutor bobExecutor;

    /*
     * Pre-requisites for the tests in this class:
     *
     * (1) hive.properties file should have this property set: hive.security=sql-standard
     * (2) tempto-configuration.yaml file should have definitions for the following connections to Presto server:
     *          - "alice@presto" that has "jdbc_user: alice"
     *          - "bob@presto" that has "jdbc_user: bob"
     *     (all other values of the connection are same as that of the default "presto" connection).
    */

    @BeforeTestWithContext
    public void setup()
    {
        tableName = "alice_owned_table";
        aliceExecutor = connectToPresto("alice@presto");
        bobExecutor = connectToPresto("bob@presto");

        aliceExecutor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        aliceExecutor.executeQuery(format("CREATE TABLE %s(month bigint, day bigint)", tableName));

        assertAccessDeniedOnAllOperationsOnTable(bobExecutor, tableName);
    }

    @Test(groups = {HIVE_CONNECTOR, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevoke()
    {
        // test GRANT
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO bob WITH GRANT OPTION", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
        aliceExecutor.executeQuery(format("GRANT INSERT, SELECT ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName))).hasRowsCount(1);
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        assertThat(() -> bobExecutor.executeQuery(format("DELETE FROM %s WHERE day=3", tableName))).
                failsWithMessage(format("Access Denied: Cannot delete from table default.%s", tableName));

        // test REVOKE
        aliceExecutor.executeQuery(format("REVOKE INSERT ON %s FROM bob", tableName));
        assertThat(() -> bobExecutor.executeQuery(format("INSERT INTO %s VALUES ('y', 5)", tableName))).
                failsWithMessage(format("Access Denied: Cannot insert into table default.%s", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        aliceExecutor.executeQuery(format("REVOKE INSERT, SELECT ON %s FROM bob", tableName));
        assertThat(() -> bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).
                failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));
    }

    @Test(groups = {HIVE_CONNECTOR, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevokeAll()
    {
        aliceExecutor.executeQuery(format("GRANT ALL PRIVILEGES ON %s TO bob", tableName));
        assertThat(bobExecutor.executeQuery(format("INSERT INTO %s VALUES (4, 13)", tableName))).hasRowsCount(1);
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        bobExecutor.executeQuery(format("DELETE FROM %s", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();

        aliceExecutor.executeQuery(format("REVOKE ALL PRIVILEGES ON %s FROM bob", tableName));
        assertAccessDeniedOnAllOperationsOnTable(bobExecutor, tableName);
    }

    @Test(groups = {HIVE_CONNECTOR, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testPublic()
    {
        aliceExecutor.executeQuery(format("GRANT SELECT ON %s TO PUBLIC", tableName));
        assertThat(bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
        aliceExecutor.executeQuery(format("REVOKE SELECT ON %s FROM PUBLIC", tableName));
        assertThat(() -> bobExecutor.executeQuery(format("SELECT * FROM %s", tableName))).
                failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableName))).hasNoRows();
    }

    private static void assertAccessDeniedOnAllOperationsOnTable(QueryExecutor queryExecutor, String tableName)
    {
        assertThat(() -> queryExecutor.executeQuery(format("SELECT * FROM %s", tableName))).
                failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));
        assertThat(() -> queryExecutor.executeQuery(format("INSERT INTO %s VALUES (3, 22)", tableName))).
                failsWithMessage(format("Access Denied: Cannot insert into table default.%s", tableName));
        assertThat(() -> queryExecutor.executeQuery(format("DELETE FROM %s WHERE day=3", tableName))).
                failsWithMessage(format("Access Denied: Cannot delete from table default.%s", tableName));
    }
}
