package com.pactera.oozieAPI.spark;

import org.apache.calcite.avatica.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created by Administrator on 2021/4/22 0022.
 */
public class HbaseRead {

    static String convertScanToString(Scan scan) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);

        return Base64.encodeBytes(out.toByteArray());
    }
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local[*]")
                //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("hadoop.home.dir", "/user/hive/warehouse")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .enableHiveSupport()
                .getOrCreate();
//        spark.sql("SELECT * FROM classes1").show();


        String FAMILY = "user";
        String COLUM_NAME = "name";
        String COLUM_AGE = "age";

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());



        Configuration hconf = HBaseConfiguration.create();
        hconf.set("hbase.zookeeper.property.clientPort", "2181");
        hconf.set("hbase.zookeeper.quorum", "10.114.10.92,10.114.10.93,10.114.10.94");
        hconf.set(TableInputFormat.INPUT_TABLE, "class");

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_NAME));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_AGE));


        try {

            //读HBase数据转化成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hbaseRDD.cache();// 对myRDD进行缓存
            long count = hbaseRDD.count();
            System.out.println("数据总条数：" + count);

            List<Result> list=hbaseRDD.map(t->t._2()).collect();
            System.out.println("list size---"+list.size());
            for(Result result:list){
                List<Cell> cells=result.listCells();
                System.out.println(Bytes.toString(CellUtil.cloneRow(cells.get(0))));
                for(Cell cell:cells){
                    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }


    }
}
