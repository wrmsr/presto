package com.wrmsr.presto.jdbc;

import com.wrmsr.presto.util.CaseInsensitiveMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JdbcUtil
{
    public static Map<String, Object> readRow(ResultSet rs, ResultSetMetaData md) throws SQLException
    {
        Map<String, Object> row = new CaseInsensitiveMap<>();
        int columns = md.getColumnCount();
        for (int i = 1; i <= columns; ++i)
            row.put(md.getColumnName(i), rs.getObject(i));
        return row;
    }

    public static Map<String, Object> readRow(ResultSet rs) throws SQLException {
        return readRow(rs, rs.getMetaData());
    }

    public static List<Map<String, Object>> readResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List<Map<String, Object>> list = new ArrayList<>(64);
        while (rs.next())
            list.add(readRow(rs, md));
        return list;
    }

    public static List<Map<String, Object>> select(Connection conn, String query,
                                                   Object... params) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < params.length; ++i)
                stmt.setObject(i + 1, params[i]);
            try (ResultSet result = stmt.executeQuery()) {
                return readResultSet(result);
            }
        }
    }

    public static Map<String, Object> one(Connection conn, String query, Object... params) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < params.length; ++i)
                stmt.setObject(i + 1, params[i]);
            try (ResultSet result = stmt.executeQuery()) {
                if (!result.next())
                    return null;
                Map<String, Object> row = readRow(result);
                // if (result.next())
                //    throw
                return row;
            }
        }
    }

    public static Object scalar(Connection conn, String query, Object... params) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < params.length; ++i)
                stmt.setObject(i + 1, params[i]);
            try (ResultSet result = stmt.executeQuery()) {
                if (!result.next())
                    return null;
                return result.getObject(1);
            }
        }
    }
}
