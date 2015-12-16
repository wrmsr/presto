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
package com.wrmsr.presto;

import org.testng.annotations.Test;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.derby.drda.NetworkServerControl;

public class TestDerbyServer
{
    @Test
    public void testSanity() throws Throwable
    {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getByName("localhost"),1527);
        server.start(null);

        String nsURL="jdbc:derby://localhost:1527/metastore_db;create=true";
        java.util.Properties props = new java.util.Properties();
        props.put("user","usr");
        props.put("password","pwd");

/*
    If you are running on JDK 1.6 or higher, then you do not
    need to invoke Class.forName(). In that environment, the
    EmbeddedDriver loads automatically.
*/
        Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
        Connection conn = DriverManager.getConnection(nsURL, props);

/*interact with Derby*/
        Statement s = conn.createStatement();

        ResultSet rs = s.executeQuery("SELECT * FROM HotelBookings");
    }
}
