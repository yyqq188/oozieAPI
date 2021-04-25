package com.pactera.oozieAPI.oozie;

import com.pactera.oozieAPI.config.HadoopConf;
import org.apache.oozie.client.WorkflowJob;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Administrator on 2021/4/20 0020.
 */
public class OozieClient {

    private final Properties properties;


    public OozieClient(Properties properties){
        this.properties = properties;
    }





    public String submitWorkflow(){

        org.apache.oozie.client.OozieClient oozieClient = new org.apache.oozie.client.OozieClient(properties.getProperty("hadoop.oozie.url"));
        Properties conf = initHiveConf(properties,oozieClient);
        try{
            String jobId = oozieClient.run(conf);
            System.out.println("Workflow job " + jobId + " submit");
            while(oozieClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING){
                System.out.println("Workflow job running ");
                Thread.sleep(10 * 1000);
            }
            System.out.println("Workflow job completed");
            return jobId;
        }catch (Exception e){
            System.out.println("Error  " + e.getLocalizedMessage());
            return "";
        }
    }



    private Properties initShellConf(Properties properties,org.apache.oozie.client.OozieClient oozieClient){
        Properties conf = oozieClient.createConfiguration();


        //指定用户
        conf.setProperty(org.apache.oozie.client.OozieClient.USER_NAME,"root");
        conf.setProperty("nameNode",properties.getProperty("hadoop.hdfs.namenode.url"));
        conf.setProperty("jobTracker",properties.getProperty("hadoop.mapreduce.jobtracker.url"));
        conf.setProperty("queueName","default");

        conf.setProperty(org.apache.oozie.client.OozieClient.APP_PATH,"hdfs://192.168.33.10:9000/user/root/oozie-apps/shell");
        conf.setProperty("EXEC", "p1.sh");
        conf.setProperty("shellFilePath", "/user/root/oozie-apps/shell/${EXEC}#${EXEC}");

        return conf;

    }

    private Properties initHiveConf(Properties properties,org.apache.oozie.client.OozieClient oozieClient){
        Properties conf = oozieClient.createConfiguration();


        //指定用户
        conf.setProperty(org.apache.oozie.client.OozieClient.USER_NAME,"root");
        conf.setProperty("nameNode",properties.getProperty("hadoop.hdfs.namenode.url"));
        conf.setProperty("jobTracker",properties.getProperty("hadoop.mapreduce.jobtracker.url"));
        conf.setProperty("queueName","default");

        conf.setProperty(org.apache.oozie.client.OozieClient.APP_PATH, properties.getProperty("hadoop.hdfs.namenode.url") +
                properties.getProperty("oozie-apps.root.path"));

        conf.setProperty("oozie.use.system.libpath","true");
        return conf;

    }









    public static void main(String[] args) throws IOException, InterruptedException {

        HadoopConf conf = new HadoopConf();
        Properties properties = conf.hadoopProperties();
        new OozieClient(properties).submitWorkflow();
    }
}
