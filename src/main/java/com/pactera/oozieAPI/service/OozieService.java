package com.pactera.oozieAPI.service;

import com.pactera.oozieAPI.config.HadoopConf;
import com.pactera.oozieAPI.gensql.GenSQL;
import com.pactera.oozieAPI.oozie.OozieClient;

import java.util.Properties;

/**
 * Created by Administrator on 2021/4/20 0020.
 */
public class OozieService {
    public void submitWorkFlow(String sqlStr,Properties properties) {

        //创建新的script.q的文件
        new GenSQL(new HadoopConf()).deleteAndGenHDFSSQLFile(
                properties.getProperty("oozie-apps.root.path"),
                sqlStr);
        //启动
        new OozieClient(properties).submitWorkflow();
    }

    public static void main(String[] args) {
        HadoopConf conf = new HadoopConf();
        Properties properties = conf.hadoopProperties();
        String sqlStr = "create table  classes7 (name string,age int) row format delimited fields terminated by ',';\n"+
                "load data inpath '/user/root/oozie-apps/hive/data' into table classes7;\n";
        new OozieService().submitWorkFlow(sqlStr,properties);
    }
}
