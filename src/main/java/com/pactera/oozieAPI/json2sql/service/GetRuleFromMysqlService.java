package com.pactera.oozieAPI.json2sql.service;

import com.pactera.oozieAPI.json2sql.utils.MysqlUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author SUN KI
 * @time 2021/4/20 18:19
 * @Desc 根据传参从mysql获取规则
 */
public class GetRuleFromMysqlService {
    public String getRuleFromMysql(int type, String name) throws Exception {
        //获取连接
        Connection conn;
        PreparedStatement ps;
        ResultSet rs;
        conn = MysqlUtils.getConnection();
        String createTypeStr = null;
        String ruleStr = null;
        //如果type是1, 从dp_user_tag中查询, 如果type是2, 从dp_user_crowd查询
        String tableName;
        String fieldName1;
        String fieldName2;
        if (type == 1){
            tableName = "dp_user_tag_test";
            fieldName1 = "create_type";
            //书写sql
            String sql1 = "select " + fieldName1 + " from " + tableName + " where `name` = " + "'" + name + "'";
            ps = conn.prepareStatement(sql1);
            //执行sql
            rs = ps.executeQuery();
            while (rs.next()){
                createTypeStr = rs.getString(1);
            }
            if ("1".equals(createTypeStr)){
                fieldName2 = "rule_content_list";
                String sql2 = "select " + fieldName2 + " from " + tableName + " where `name` = " + "'" + name + "'";
                ps = conn.prepareStatement(sql2);
                //执行sql
                rs = ps.executeQuery();
                while (rs.next()){
                    ruleStr = rs.getString(1);
                }
            }else {
                fieldName2 = "rule_sql";
                String sql2 = "select " + fieldName2 + " from " + tableName + " where `name` = " + "'" + name + "'";
                ps = conn.prepareStatement(sql2);
                //执行sql
                rs = ps.executeQuery();
                while (rs.next()){
                    ruleStr = rs.getString(1);
                }
            }
        }else {
            tableName = "dp_user_crowd";
            fieldName1 = "rule_content";
            //书写sql
            String sql1 = "select " + fieldName1 + " from " + tableName + " where `name` = " + "'" + name + "'";
            ps = conn.prepareStatement(sql1);
            //执行sql
            rs = ps.executeQuery();
            while (rs.next()){
                ruleStr = rs.getString(1);
            }
        }
        //关闭资源
        MysqlUtils.closeConnection(rs,ps,conn);
        return ruleStr;
    }
}
