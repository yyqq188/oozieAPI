package com.pactera.oozieAPI.json2sql.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveJDBCUtils {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://10.114.10.94:10000/default";//端口默认10000
    private static String user = "";
    private static String password = "";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    // 加载驱动、创建连接
    public Statement Connection() throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url,user,password);
        stmt = conn.createStatement();
        return stmt;
    }



    // 创建数据库
    public static void createDatabase() throws Exception {
        String sql = "create database databaseName";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 查询所有数据库
    public static void showDatabases() throws Exception {
        String sql = "show databases";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    // 创建表（分割符为“，”）
    public static void createTable() throws Exception {
        String sql="create table tableName(name string,sex string) row format delimited fields terminated by ','";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 查询所有表
    public static void showTables() throws Exception {
        String sql = "show tables";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    // 查看表结构
    public static void descTable() throws Exception {
        String sql = "desc formatted tableName";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }
    }

    // 加载数据(请确保文件权限)
    public static void loadData() throws Exception {
        String filePath = "file路径";
        String sql = "load data inpath '" + filePath + "' into table tableName";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 查询数据
    public static void selectData(String str) throws Exception {
        String sql = "select * from tableName";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    // 删除数据库
    public static void dropDatabase() throws Exception {
        String sql = "drop database if exists tableName";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 删除数据库表
    public static void deopTable() throws Exception {
        String sql = "drop table if exists tableName";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 释放资源
    public static void destory() throws Exception {
        if ( rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    public static void main(String[] args) throws Exception {
        HiveJDBCUtils h = new HiveJDBCUtils();
        stmt = h.Connection();


        String tmpsql = "create table classes1 (id int,name string,age int) row format delimited fields terminated by ','";

        String tmpsql2 = "load data inpath '/user/root/oozie-apps/hive/data' overwrite into table classes1";

        String sql = "create external table classes3(id int, name string, age int) \n" +
                "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' \n" +
                "WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,user:name,user:age\") \n" +
                "TBLPROPERTIES(\"hbase.table.name\" = \"class\")";


        String sql1 = "insert overwrite table classes3 select * from classes1";


        String sql2 = "drop table classes2";

        stmt.execute(sql1);
        stmt.close();
    }
}
