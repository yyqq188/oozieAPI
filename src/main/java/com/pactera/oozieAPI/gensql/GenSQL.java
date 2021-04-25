package com.pactera.oozieAPI.gensql;

import com.pactera.oozieAPI.config.HadoopConf;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Administrator on 2021/4/20 0020.
 */
public class GenSQL {
    private final HadoopConf hadoopConf;
    public GenSQL(HadoopConf hadoopConf){
        this.hadoopConf = hadoopConf;
    }


    public void genLocalSQLFile(String fileDirPath,String sqlStr){

        String fileName= "script.q";
        File sqlFile = new File(fileDirPath+ File.separator + fileName);
        if(sqlFile.exists()) {
            sqlFile.delete();
        }

        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(fileDirPath+File.separator + fileName));
            out.write(sqlStr);
            out.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void genHDFSSQLFile(String fileDirPath,String sqlStr) {
        Properties properties = hadoopConf.hadoopProperties();
        HadoopClient client = new HadoopClient("root",properties.getProperty("hadoop.conf.path"));
        FileSystem fileSystem = null;
        String filePath = fileDirPath + "/script.q";
        try{
            fileSystem = client.getFileSystem(null,properties.getProperty("hadoop.hdfs.namenode.url"));
            FSDataOutputStream outputStream = fileSystem.create(new Path(properties.getProperty("hadoop.hdfs.namenode.url")  +  filePath));
            System.out.println(properties.getProperty("hadoop.hdfs.namenode.url")  +  filePath);
            outputStream.write(sqlStr.getBytes());
            outputStream.flush();
            outputStream.close();

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    //删除并创建
    public void deleteAndGenHDFSSQLFile(String fileDirPath,String sqlStr) {
        Properties properties = hadoopConf.hadoopProperties();
        HadoopClient client = new HadoopClient("root",properties.getProperty("hadoop.conf.path"));
        FileSystem fileSystem = null;
        String fileName = fileDirPath + "/script.q";
        Path filePath = new Path(properties.getProperty("hadoop.hdfs.namenode.url")  +  fileName);
        try{
            fileSystem = client.getFileSystem(null,properties.getProperty("hadoop.hdfs.namenode.url"));

            fileSystem.delete(filePath,true);
            FSDataOutputStream outputStream = fileSystem.create(filePath);
            outputStream.write(sqlStr.getBytes());
            outputStream.flush();
            outputStream.close();

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        new GenSQL(new HadoopConf()).deleteAndGenHDFSSQLFile("/user/root/oozie-apps/hive",
                "create external table  if not exists plc_data22 (name string,age int) row format delimited fields terminated by ','stored as textfile;");
    }
}
