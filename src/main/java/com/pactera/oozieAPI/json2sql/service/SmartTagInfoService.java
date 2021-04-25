package com.pactera.oozieAPI.json2sql.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pactera.oozieAPI.config.HadoopConf;
import com.pactera.oozieAPI.service.OozieService;
import com.pactera.oozieAPI.spark.HbaseWrite;
import com.pactera.oozieAPI.spark.Hive2Hbase;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @author SUN KI
 * @time 2021/4/13 13:14
 * @Desc 用户标签service业务层处理
 */
public class SmartTagInfoService {
    //private static Logger logger = LoggerFactory.getLogger(SmartTagInfoService.class);
    /**
     * 运行用户标签，将返回数据插入到数据库
     *
     * @param jsonStr
     * @return ruleSql
     */
    public JSONObject runTagByJsonStr(String jsonStr) {//拿到的就是ruleContentList

        LayerService layerService = new LayerService();
        JSONObject ruleSql = new JSONObject();
        /*JSONArray jsonStrArr = JSON.parseArray(jsonStr);
        JSONObject request = JSON.parseObject(jsonStrArr.getString(0));
        System.out.println(request);
        String ruleContentList = request.getString("ruleContentList");*/
        if (jsonStr.startsWith("[")) {
            JSONArray jsonArray = JSON.parseArray(jsonStr);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = JSON.parseObject(jsonArray.get(i).toString());
                String value = jsonObject.getString("value");//分层名称
                ruleSql.put(value, layerService.buildLayeredRuleSql(jsonArray.get(i).toString()));
            }
        } else {
            JSONObject resultSqlJsonObj = JSON.parseObject(jsonStr);
            ruleSql.put("sql", resultSqlJsonObj);
        }
        System.out.println(ruleSql);
        //根据规则拼接sql，首先拼接用户属性相关sql，数据类型还有标签
        return ruleSql;
    }




    public void runTag(String jsonStr){

        HadoopConf conf = new HadoopConf();
        Properties properties = conf.hadoopProperties();
//        String sqlStr = "create table  classes7 (name string,age int) row format delimited fields terminated by ',';\n"+
//                "load data inpath '/user/root/oozie-apps/hive/data' into table classes7;\n";
        ExecutorService executorService = Executors.newCachedThreadPool();
        for(Map.Entry entry : runTagByJsonStr(jsonStr).entrySet()){
            executorService.execute(new Runnable() {
                public void run() {
                    new OozieService().submitWorkFlow(entry.getValue().toString(),properties);
                }
            });

        }


        executorService.shutdown();

    }




    public void runTagSpark(String jsonStr,int type,String typeName,String tableName,String columnName){
        ExecutorService executorService = Executors.newCachedThreadPool();
        for(Map.Entry entry : runTagByJsonStr(jsonStr).entrySet()){
            executorService.execute(new Runnable() {
                public void run() {
                    System.out.println(entry.getKey());
                    System.out.println();
                    String sqlStr = JSONObject.parseObject(entry.getValue().toString()).getString("resultSql");
                    String sqlStr2 = sqlStr.replace("from event","from hb_event_data")
                            .replace("from user","from hb_user_info");
                    System.out.println(sqlStr);
                    System.out.println(sqlStr2);

                    new Hive2Hbase().hive2hbase(entry.getKey().toString(),sqlStr2,type,typeName,tableName,columnName);
                }
            });

        }


        executorService.shutdown();

    }
}
