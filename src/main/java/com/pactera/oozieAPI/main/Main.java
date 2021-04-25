package com.pactera.oozieAPI.main;

import com.pactera.oozieAPI.config.HadoopConf;
import com.pactera.oozieAPI.json2sql.service.GetRuleFromMysqlService;
import com.pactera.oozieAPI.json2sql.service.SmartTagInfoService;
import com.pactera.oozieAPI.json2sql.service.UserCrowdedInfoService;
import com.pactera.oozieAPI.json2sql.utils.StringUtils;

import java.util.Properties;

/**
 * Created by Administrator on 2021/4/22 0022.
 */
public class Main {



    public void mainFunc(int type,String name) throws Exception{

        Properties properties = new HadoopConf().hadoopProperties();

        GetRuleFromMysqlService getRuleFromMysqlService = new GetRuleFromMysqlService();
        SmartTagInfoService smartTagInfoService = new SmartTagInfoService();
        UserCrowdedInfoService userCrowdedInfoService = new UserCrowdedInfoService();

        String ruleStr = getRuleFromMysqlService.getRuleFromMysql(type,name);

        if (StringUtils.isNotBlank(ruleStr)){
            if (ruleStr.startsWith("{")){
                String tableNameCrow=properties.getProperty("tableNameCrow");
                String columnFamilyCrow=properties.getProperty("columnFamilyCrow");

                userCrowdedInfoService.runTagSpark(ruleStr,type,name,tableNameCrow,columnFamilyCrow);
            }else {
                String tableNameTag=properties.getProperty("tableNameTag");
                String columnFamilyTag=properties.getProperty("columnFamilyTag");
                smartTagInfoService.runTagSpark(ruleStr,type,name,tableNameTag,columnFamilyTag);
            }
        }
    }







    public static void main(String[] args) {
        try{
            new Main().mainFunc(1, "user_tag_6");
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
