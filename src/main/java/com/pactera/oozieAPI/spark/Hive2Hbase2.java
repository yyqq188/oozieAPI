package com.pactera.oozieAPI.spark;

import com.pactera.oozieAPI.json2sql.utils.HiveJDBCUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Administrator on 2021/4/23 0023.
 */
public class Hive2Hbase2 {


    public void mainFunc(String layName,String sqlStr,int type,String typeName,String tableName,String columneFamily) throws Exception{
        HiveJDBCUtils h = new HiveJDBCUtils();
        Statement stmt = h.Connection();
        List<String> sqls = new ArrayList<>();

        Date date = new Date();
        String strDateFormat = "yyyy-MM-dd";
        SimpleDateFormat sdf = new SimpleDateFormat(strDateFormat);
        String dateF = sdf.format(date);

//        String tmpsql = "create table classes1 (id int,name string,age int) row format delimited fields terminated by ','";
//        String tmpsql2 = "load data inpath '/user/root/oozie-apps/hive/data' overwrite into table classes1";


        String tempTable = "tempTable1";
        String sql1 = MessageFormat.format("create external table {0}(key_id string, user_code string, s_date string,{1} string) \n" +
                "STORED BY \"org.apache.hadoop.hive.hbase.HBaseStorageHandler\" \n" +
                "WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,{2}:{3},{4}:{5},{6}:{7}\") \n" +
                "TBLPROPERTIES(\"hbase.table.name\" = \"{8}\")",
                tempTable,typeName.toLowerCase(),
                columneFamily,"USER_CODE",columneFamily,"S_DATE",columneFamily,typeName.toUpperCase(),tableName);


        //limit要去掉     目前这里的sql还要再基于传来的sql进行修改，传入的sql的  'select user_code from' 字段不用变，之后的字符直接追加
        String sql2 = MessageFormat.format("insert overwrite table {0} " +
                "select concat_ws(\"@\",code,\"{1}\"),code,\"{2}\",\"{3}\" from portrait.hb_user_info limit 2,6",tempTable,dateF,dateF,layName);


        String drop1 = MessageFormat.format("drop table {0}",tempTable);

        sqls.add(sql1);
        sqls.add(sql2);
        sqls.add(drop1);
        for(String sqlstr:sqls){
            try{
                stmt.execute(sqlstr);
                System.out.println("SQL:" + sqlstr + " 执行成功");
            }catch (SQLException e){
                e.printStackTrace();
                System.out.println("SQL:" + sqlstr + " 执行错误");
                break;
            }
        }
        stmt.close();

    }
    public static void main(String[] args) throws Exception {

        new Hive2Hbase2().mainFunc(
                "男","",1,"USER_TAG_1","PORTRAIT:HB_TAG_RESULT","F_TAG_RESULT");

    }
}
