package com.pactera.oozieAPI.main;

import com.pactera.oozieAPI.json2sql.service.GetRuleFromMysqlService;
import com.pactera.oozieAPI.json2sql.service.SmartTagInfoService;
import com.pactera.oozieAPI.json2sql.service.UserCrowdedInfoService;
import com.pactera.oozieAPI.json2sql.utils.StringUtils;

/**
 * Created by Administrator on 2021/4/21 0021.
 */
public class MainBak {
    public static void main(String[] args) throws Exception{
        GetRuleFromMysqlService getRuleFromMysqlService = new GetRuleFromMysqlService();
        SmartTagInfoService smartTagInfoService = new SmartTagInfoService();
        UserCrowdedInfoService userCrowdedInfoService = new UserCrowdedInfoService();
        //String jsonStr = getProfileRule_1();
        //String jsonStr = getProfileRule_2();
        //String jsonStr = getProfileRule_3();
        //String jsonStr = getProfileRule_4();
        //String jsonStr = getProfileRule_5();
        //String jsonStr = getProfileRule_6();
        //String jsonStr = getProfileRule_7();
        //String jsonStr = getProfileRule_8();
        //String jsonStr = getEventRule_1();
        //String jsonStr = getEventRule_2();
        //String jsonStr = getEventRule_3();
        //String jsonStr = getEventRule_4();
        //String jsonStr = getEventRule_5();
        //String jsonStr = getEventRule_6();
        //String jsonStr = getEventRule_7();
        //String jsonStr = getEventRule_8();
        //String jsonStr = getEventRule_9();
        //String jsonStr = getEventRule_10();
        //String jsonStr = getEventRule_11();
        //String jsonStr = getAllRule_1();
        String ruleStr = getRuleFromMysqlService.getRuleFromMysql(1, "user_tag_6");

        if (StringUtils.isNotBlank(ruleStr)){
            if (ruleStr.startsWith("{")){
                userCrowdedInfoService.runTagByJsonStr(ruleStr);
//                userCrowdedInfoService.runTag(ruleStr);
            }else {
                smartTagInfoService.runTagByJsonStr(ruleStr);
//                smartTagInfoService.runTag(ruleStr);
            }
        }
    }
}
