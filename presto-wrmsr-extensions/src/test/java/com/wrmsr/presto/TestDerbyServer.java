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

        String nsURL="jdbc:derby://localhost:1527/sample";
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
