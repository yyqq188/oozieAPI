package com.pactera.oozieAPI.spark;

import org.apache.calcite.avatica.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2021/4/22 0022.
 */
public class HbaseWrite {

    private static void hBaseWriter(Iterator<Row> iterator) throws IOException{
        String tableName = "class";
        String columnFamily = "user";
        Configuration conf = HBaseConfiguration.create();
        Connection connection = null;
        Table table = null;



        try{
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));
            List<Put> putList = new ArrayList<Put>();
            while (iterator.hasNext()) {
                Row item = iterator.next();
                    if(item.size() == 3){
                    Put put = new Put(String.valueOf(item.get(0)).getBytes());
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("name"), Bytes.toBytes(String.valueOf(item.get(1))));
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(item.get(2))));
                    putList.add(put);
                }
                System.out.println(item);

            }
            if(putList.size() > 0){
                table.put(putList);
            }

        }catch (Exception e){
            e.printStackTrace();
        }


    }






    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Hive2Hbase")
                .master("local[*]")
                //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("hadoop.home.dir", "/user/hive/warehouse")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .enableHiveSupport()
                .getOrCreate();
        Dataset<Row> sqlData = spark.sql("SELECT * FROM classes1");

        sqlData.toJavaRDD().foreachPartition(
                new VoidFunction<Iterator<Row>>() {
                    public void call(Iterator<Row> iterator) throws Exception {
                        hBaseWriter(iterator);
                    }
                }
        );

    }
}
