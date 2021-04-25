package com.pactera.oozieAPI.json2sql.utils;

import java.sql.*;

/**
 * @author SUN KI
 * @time 2021/4/20 19:08
 * @Desc mysql连接工具类
 */
public class MysqlUtils {
    private static final String URL = "jdbc:mysql://192.168.8.20:3306/pactera-cloud";
    private static final String USER = "pactera";
    private static final String PASSWORD = "pactera1234";
    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("加载Mysql数据库驱动失败!");
        }
    }

    /**
     * 获取Connection
     *
     * @return conn
     * @throws SQLException
     * @throws ClassNotFoundException
     */

    public static Connection getConnection() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            System.out.println("获取数据库连接失败!");
            throw e;
        }
        return conn;
    }

    /**
     * 关闭ResultSet
     *
     * @param rs
     */

    public static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     * 关闭Statement
     *
     * @param stmt
     */

    public static void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     * 关闭ResultSet、Statement
     *
     * @param rs
     * @param stmt
     */

    public static void closeStatement(ResultSet rs, Statement stmt) {
        closeResultSet(rs);
        closeStatement(stmt);
    }

    /**
     * 关闭PreparedStatement
     *
     * @param pstmt
     * @throws SQLException
     */

    public static void fastcloseStmt(PreparedStatement pstmt) throws SQLException {
        pstmt.close();
    }

    /**
     * 关闭ResultSet、PreparedStatement
     *
     * @param rs
     * @param pstmt
     * @throws SQLException
     */

    public static void fastcloseStmt(ResultSet rs, PreparedStatement pstmt) throws SQLException {
        rs.close();
        pstmt.close();
    }

    /**
     * 关闭ResultSet、Statement、Connection
     *
     * @param rs
     * @param pstmt
     * @param con
     */

    public static void closeConnection(ResultSet rs, PreparedStatement pstmt, Connection con) {
        closeResultSet(rs);
        closeStatement(pstmt);
        closeConnection(con);
    }

    /**
     * 关闭Statement、Connection
     *
     * @param stmt
     * @param con
     */

    public static void closeConnection(Statement stmt, Connection con) {
        closeStatement(stmt);
        closeConnection(con);
    }

    /**
     * 关闭Connection
     *
     * @param con
     */

    public static void closeConnection(Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
