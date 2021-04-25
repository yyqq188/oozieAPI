package com.pactera.oozieAPI.gensql;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

/**
 * Created by Administrator on 2021/4/20 0020.
 */
public class HadoopClient {


    private Configuration configuration;
    private String proxyUser;
    private UserGroupInformation ugi;

    public HadoopClient(String proxyUser,String hadoopConfPath){
        this.proxyUser = proxyUser;
        ugi = UserGroupInformation.createRemoteUser(proxyUser);
        this.configuration = new Configuration();
        configuration.addResource(new Path(String.format("%s/hadoop-site.xml",hadoopConfPath)));
        configuration.addResource(new Path(String.format("%s/core-site.xml",hadoopConfPath)));


    }

    public <T> T doPrivileged(PrivilegedExceptionAction<T> action,String realUser) throws IOException, InterruptedException {
        if(Strings.isNullOrEmpty(realUser)){
            return ugi.doAs(action);
        }
        UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(realUser,ugi);
        return proxyUser.doAs(action);
    }

    public FileSystem getFileSystem(String realUser, final String hdfsUri) throws Exception{
        return doPrivileged(
                new PrivilegedExceptionAction<FileSystem>() {

                    public FileSystem run() throws Exception {
                        return FileSystem.newInstance(URI.create(hdfsUri),configuration);
                    }
                },realUser
        );

    }



    public static void main(String[] args) throws Exception {
        HadoopClient client = new HadoopClient("root","/etc/alternatives/hadoop-conf/");
        FileSystem fileSystem = client.getFileSystem(null,"hdfs://10.114.10.92:8020");


        FSDataOutputStream outputStream = fileSystem.create(new Path("hdfs://10.114.10.92:8020/tmp/sqlyhl.q"));
        outputStream.write("insss".getBytes());
        outputStream.flush();
        outputStream.close();
    }
}
