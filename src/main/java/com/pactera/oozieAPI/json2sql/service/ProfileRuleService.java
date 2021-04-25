package com.pactera.oozieAPI.json2sql.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pactera.oozieAPI.json2sql.contants.RuleJsonConstant;
import com.pactera.oozieAPI.json2sql.utils.StringUtils;

/**
 * @author SUN KI
 * @time 2021/4/13 9:08
 * @Desc 属性规则解析
 */
public class ProfileRuleService {
    private FilterService filterService = new FilterService();
    StringBuilder resultSqlTemp = new StringBuilder();


    /**
     * 用户属性解析入口
     * @param rules
     * @param relation
     * @param level
     * @param filterNumber
     * @return sqlSb
     */
    public String bulidUserInfoRuleSql(String rules, String relation, int level, int filterNumber) {

        StringBuilder sqlSb = new StringBuilder();
        JSONArray jsonArray = JSON.parseArray(rules);
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = JSONObject.parseObject(jsonArray.getString(i));
            String type = jsonObject.getString("type");//类型,rules_relation:关系,profile_rule：属性，event_rule：事件
            int filterIndex = i + 1;//过滤条件索引
            if (RuleJsonConstant.VALUE_RULE_RELATION.equals(type)) {//说明含有子规则
                String sub_relation = jsonObject.getString("relation");//关系  and
                String childRules = jsonObject.getString("rules");//子规则
                String resultSql = bulidUserInfoRuleSql(childRules, sub_relation, level + 1, filterNumber);
                if (level == 2 && filterIndex != jsonArray.size()){
                    sqlSb.append(resultSql + RuleJsonConstant.addBlank(relation));
                }else {
                    sqlSb.append(resultSql);
                }

            } else if (RuleJsonConstant.VALUE_PROFILE_RULE.equals(type)) {//过滤条件
                String childFiled = jsonObject.getString("field");//字段

                String tableName = "";//表名
                if (StringUtils.isNotBlank(childFiled)) {
                    String[] fieldArr = childFiled.split("\\.");
                    tableName = fieldArr[0];
                }
                //基本查询语句和过滤条件
                String sql = filterService.getBaseRuleSql(jsonObject);
                if (i == 0 && filterNumber == 1 && "".equals(resultSqlTemp.toString())) {//如果是第一个，需要加select指定表的字段
                    sqlSb.append("select user_code from " + tableName);
                    sqlSb.append(" where ");
                }
                if (level == 3) {//第三层过滤sql
                    if (i == 0) { //如果是第三层, 那么肯定包含不止一个profile_rule
                        sqlSb.append(RuleJsonConstant.LEFT_PARENTHESIS);
                    }
                    if (i != jsonArray.size() - 1) {
                        sqlSb.append(sql + RuleJsonConstant.addBlank(relation));
                    } else {
                        sqlSb.append(sql + RuleJsonConstant.RIGHT_PARENTHESIS);
                    }
                } else if (level == 2 && jsonArray.size() > 1) {//第二层过滤
                    if (i != jsonArray.size() - 1) {
                        sqlSb.append(sql + RuleJsonConstant.addBlank(relation));
                    } else {
                        sqlSb.append(sql);
                    }
                } else {//如果是第一层, 肯定只有一个profile_rule, 没有任何关系拼接.
                    sqlSb.append(sql);
                }
                filterNumber++;
            }
        }
        resultSqlTemp.append(sqlSb);
        return sqlSb.toString();
    }
}
