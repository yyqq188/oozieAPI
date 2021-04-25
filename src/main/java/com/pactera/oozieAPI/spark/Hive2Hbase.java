package com.pactera.oozieAPI.spark;

import com.pactera.oozieAPI.config.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 2021/4/22 0022.
 */
public class Hive2Hbase implements Serializable {


    private static void hBaseWriter(Iterator<Row> iterator,String layName,int type,String typeName,String tableName,String columnFamily) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = null;
        Table table = null;

        try{
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));
            List<Put> putList = new ArrayList<Put>();
            while (iterator.hasNext()) {
                Row item = iterator.next();
                if(item.size() == 1){
                    String userCode = String.valueOf(item.get(0));
                    Date date = new Date();
                    String strDateFormat = "yyyy-MM-dd";
                    SimpleDateFormat sdf = new SimpleDateFormat(strDateFormat);
                    String dateF = sdf.format(date);
                    Put put = new Put((userCode +"@"+ dateF).getBytes());
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(typeName), Bytes.toBytes(layName));
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("USER_CODE"), Bytes.toBytes(userCode));
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("S_DATE"), Bytes.toBytes(dateF));
                    putList.add(put);
                }
                System.out.println(item);

            }
            if(putList.size() > 0){
                table.put(putList);
                table.close();
            }

        }catch (Exception e){
            e.printStackTrace();
        }


    }


    public void hive2hbase(String layName,String sqlStr,int type,String typeName,String tableName,String columnName){
        SparkSession spark = SparkSession
                .builder()
                .appName("Hive2Hbase")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/portrait.db")
//                .config("hadoop.home.dir", "/user/hive/warehouse/portrait.db")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .enableHiveSupport()
                .getOrCreate();

//        spark.sparkContext().addJar("D:\\Users\\Downloads\\hive-hbase-handler-2.1.1-cdh6.3.2.jar");

        Dataset<Row> sqlData = spark.sql(sqlStr);
        sqlData.toJavaRDD().foreachPartition(
                new VoidFunction<Iterator<Row>>() {
                    public void call(Iterator<Row> iterator) throws Exception {
                        hBaseWriter(iterator,layName,type,typeName,tableName,columnName);
                    }
                }
        );
    }

    public static void main(String[] args) {
        String sqlStr = "select\n" +
                " tt21.user_code\n" +
                "from\n" +
                " (\n" +
                " select\n" +
                "  tt31.user_code\n" +
                " from\n" +
                "  (\n" +
                "  select\n" +
                "   user_code\n" +
                "  from\n" +
                "   portrait.hb_event_data\n" +
                "  where\n" +
                "   create_time between '1616641222' and '1616641647'\n" +
                "   and user_code in (\n" +
                "   select\n" +
                "    user_code \n" +
                "   from\n" +
                "    portrait.hb_event_data tf1\n" +
                "   where\n" +
                "    tf1.ip = '192.168.1.1')\n" +
                "   and event_name = 'AppClick'\n" +
                "  group by\n" +
                "   user_code\n" +
                "  having\n" +
                "   count(1) >= '1' ) tt31\n" +
                " inner join (\n" +
                "  select\n" +
                "   user_code\n" +
                "  from\n" +
                "   portrait.hb_event_data\n" +
                "  where\n" +
                "   create_time between '1616641222' and '1616641647'\n" +
                "   and user_code in (\n" +
                "   select\n" +
                "    user_code\n" +
                "   from\n" +
                "    portrait.hb_event_data tf1\n" +
                "   where\n" +
                "    tf1.sdk = 'V1.2'\n" +
                "    and tf1.ip = '192.168.1.2')\n" +
                "   and event_name = 'App浏览页面'\n" +
                "  group by\n" +
                "   user_code\n" +
                "  having\n" +
                "   count(1) >= '1' ) tt32 on\n" +
                "  tt31.user_code = tt32.user_code ) tt21\n" +
                "union\n" +
                "select\n" +
                " tt22.user_code\n" +
                "from\n" +
                " (\n" +
                " select\n" +
                "  user_code\n" +
                " from\n" +
                "  portrait.hb_event_data\n" +
                " where\n" +
                "  create_time between '1616641222' and '1616641647'\n" +
                "  and user_code in (\n" +
                "  select\n" +
                "   user_code\n" +
                "  from\n" +
                "   portrait.hb_event_data tf1\n" +
                "  where\n" +
                "   tf1.ip = '192.168.1.3')\n" +
                "  and event_name = 'App元素点击'\n" +
                " group by\n" +
                "  user_code\n" +
                " having\n" +
                "  count(1) >= '1'\n" +
                "union\n" +
                " select\n" +
                "  user_code\n" +
                " from\n" +
                "  portrait.hb_event_data\n" +
                " where\n" +
                "  create_time between '1616641222' and '1616641647'\n" +
                "  and user_code in (\n" +
                "  select\n" +
                "   user_code\n" +
                "  from\n" +
                "   portrait.hb_event_data tf1\n" +
                "  where\n" +
                "   tf1.sdk = 'V1.4'\n" +
                "   and tf1.ip = '192.168.1.4')\n" +
                "  and event_name = 'APP崩溃'\n" +
                " group by\n" +
                "  user_code\n" +
                " having\n" +
                "  count(1) >= '1' ) tt22\n" +
                "union\n" +
                "select\n" +
                " user_code\n" +
                "from\n" +
                " portrait.hb_event_data\n" +
                "where\n" +
                " create_time between '1616641222' and '1616641647'\n" +
                " and user_code in (\n" +
                " select\n" +
                "  user_code\n" +
                " from\n" +
                "  portrait.hb_event_data tf1\n" +
                " where\n" +
                "  tf1.ip = '192.168.1.5')\n" +
                " and event_name = 'App启动'\n" +
                "group by\n" +
                " user_code\n" +
                "having\n" +
                " count(1) >= '1'\n" +
                "union\n" +
                "select\n" +
                " user_code\n" +
                "from\n" +
                " portrait.hb_event_data\n" +
                "where\n" +
                " create_time between '1616641222' and '1616641647'\n" +
                " and user_code in (\n" +
                " select\n" +
                "  user_code\n" +
                " from\n" +
                "  portrait.hb_event_data tf1\n" +
                " where\n" +
                "  tf1.ip = '192.168.1.6')\n" +
                " and event_name = 'App退出'\n" +
                "group by\n" +
                " user_code\n" +
                "having\n" +
                " count(1) >= '1'";

        String sqlStr2 = "select user_code from portrait.hb_event_data";




        new Hive2Hbase().hive2hbase("男",sqlStr2,1,"USER_TAG_1","PORTRAIT:HB_TAG_RESULT","F_TAG_RESULT");

    }
}
