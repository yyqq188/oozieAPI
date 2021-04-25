package com.pactera.oozieAPI.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Administrator on 2021/4/20 0020.
 */
public class HadoopConf {

    public Properties hadoopProperties(){
        InputStream in = HadoopConf.class.getResourceAsStream("/hadoop.properties");
        Properties properties = new Properties();
        try{
            properties.load(in);
        }catch (IOException e){
            e.printStackTrace();
        }

        return properties;

    }


    public static void main(String[] args) throws IOException {
        InputStream in = HadoopConf.class.getResourceAsStream("/hadoop.properties");
        Properties properties = new Properties();
        properties.load(in);
        String citys = properties.getProperty("hadoop.hdfs.namenode.url");
        System.out.println(citys);

    }
}
