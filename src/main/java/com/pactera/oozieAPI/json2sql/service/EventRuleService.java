package com.pactera.oozieAPI.json2sql.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pactera.oozieAPI.json2sql.contants.RuleJsonConstant;
import com.pactera.oozieAPI.json2sql.utils.StringUtils;

/**
 * @author SUN KI
 * @time 2021/4/13 13:14
 * @Desc 行为规则解析
 */
public class EventRuleService {

    private FilterService filterService = new FilterService();
    int filterNumber = 0; //给第三层表起别名用

    /**
     * 用户行为解析入口
     *
     * @param rules
     * @param relation
     * @param level
     * @return sqlSb
     */
    public String buildEventRuleSql(String rules, String relation, int level) {

        StringBuilder sqlSb = new StringBuilder();
        // json串（子串）
        JSONArray jsonArray = JSON.parseArray(rules);
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = JSONObject.parseObject(jsonArray.getString(i));
            String type = jsonObject.getString(RuleJsonConstant.KEY_TYPE);
            int filterIndex = i + 1;
            int filterSort = i + filterNumber;
            String tableAliasNow = "tt" + level + filterIndex;//表别名;
            String tableAliasPre = "tt" + level + (filterIndex - 1);
            if (RuleJsonConstant.VALUE_RULE_RELATION.equals(type)) {//说明含有子规则
                String sub_rules = jsonObject.getString(RuleJsonConstant.KEY_RULES);//子规则
                String sub_relation = jsonObject.getString(RuleJsonConstant.KEY_RELATION);//关系
                String resultSql = buildEventRuleSql(sub_rules, sub_relation, level + 1);

                if (RuleJsonConstant.RELATION_AND.equals(relation)) {//交集
                    if (level != 1 && jsonArray.size() > 1) {
                        if (filterIndex == 1) {//第一个需要指定查询的table名和列名
                            sqlSb.append("select " + tableAliasNow + ".user_code from " + RuleJsonConstant.LEFT_PARENTHESIS);
                            sqlSb.append(resultSql);
                            sqlSb.append(RuleJsonConstant.RIGHT_PARENTHESIS + RuleJsonConstant.BLANK + tableAliasNow);

                        }else {
                            sqlSb.append(RuleJsonConstant.LEFT_PARENTHESIS);
                            sqlSb.append(resultSql);
                            sqlSb.append(RuleJsonConstant.RIGHT_PARENTHESIS + RuleJsonConstant.BLANK + tableAliasNow);
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_ON) + tableAliasPre + ".user_code" + RuleJsonConstant.addBlank(RuleJsonConstant.EQUAL) + tableAliasNow + ".user_code");
                        }
                        if (i != jsonArray.size() - 1) {
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_INNER_JOIN));
                        }
                    } else {
                        sqlSb.append(resultSql);
                    }
                } else {
                    if (level != 1 && jsonArray.size() > 1){
                        sqlSb.append("select " + tableAliasNow + ".user_code from " + RuleJsonConstant.LEFT_PARENTHESIS);
                        sqlSb.append(resultSql);
                        sqlSb.append(RuleJsonConstant.RIGHT_PARENTHESIS + RuleJsonConstant.BLANK + tableAliasNow);
                        if (i != jsonArray.size() - 1) {
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_UNION));
                        }
                    }else {
                        sqlSb.append(resultSql);
                    }
                }
            } else if (RuleJsonConstant.VALUE_EVENT_RULE.equals(type)) {//主要事件信息
                String timeParams = jsonObject.getString(RuleJsonConstant.KEY_TIME_PARAMS);
                JSONArray timeParamsArr = JSON.parseArray(timeParams);
                if (level == 3) {
                    tableAliasNow = "tt" + level + (filterNumber + filterSort + 1);//表别名
                    tableAliasPre = "tt" + level + (filterNumber + filterSort);
                    if (RuleJsonConstant.RELATION_AND.equals(relation)){
                        if (i == 0) {
                            sqlSb.append("select " + tableAliasNow + ".user_code from ");
                            sqlSb.append(RuleJsonConstant.LEFT_PARENTHESIS + "select user_code from event where ");
                        } else {
                            sqlSb.append("select user_code from event where ");
                        }
                    }else {
                        sqlSb.append("select user_code from event where ");
                    }

                    sqlSb.append(RuleJsonConstant.KEY_TIME_PARAMS + RuleJsonConstant.addBlank("between") + "'" + timeParamsArr.get(0) + "'" + RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_AND) + "'" + timeParamsArr.get(1) + "'");

                    //判断添加筛选只添加了一条还是多条
                    String filters = jsonObject.getString(RuleJsonConstant.KEY_FILTERS);//子规则
                    if (StringUtils.isNotBlank(filters)) {
                        sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_AND) + "user_code");
                        sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.IN) + RuleJsonConstant.LEFT_PARENTHESIS);
                        JSONArray filterArray = JSON.parseArray(filters);
                        JSONObject filterJsonObj = JSONObject.parseObject(filterArray.getString(0));
                        String filterType = filterJsonObj.getString(RuleJsonConstant.KEY_TYPE);
                        if (RuleJsonConstant.VALUE_FILTER.equals(filterType)) {//一条filter
                            sqlSb.append(filterService.getBaseFilterSql(filterJsonObj));
                        } else {//多条filter即含有subFilter
                            sqlSb.append(filterService.getBaseSubFilterSql(filterJsonObj));
                        }
                        sqlSb.append(RuleJsonConstant.RIGHT_PARENTHESIS);
                    }

                    //判断isDone, 是否做过
                    String isDone = jsonObject.getString(RuleJsonConstant.KEY_IS_DONE);
                    if (RuleJsonConstant.IS_DONE_FLAG.equals(isDone)) {
                        sqlSb.append(filterService.getMeasureInfoSql(jsonObject));
                    } else {
                        String measure = jsonObject.getString(RuleJsonConstant.KEY_MEASURE);
                        JSONObject measureJsonObj = JSONObject.parseObject(measure);
                        String eventName = measureJsonObj.getString(RuleJsonConstant.KEY_EVENT_NAME);
                        sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_AND) + RuleJsonConstant.KEY_EVENT_NAME + RuleJsonConstant.addBlank(RuleJsonConstant.EQUAL) + eventName);
                    }

                    if (RuleJsonConstant.RELATION_AND.equals(relation)) {
                        sqlSb.append(RuleJsonConstant.RIGHT_PARENTHESIS + RuleJsonConstant.BLANK + tableAliasNow);
                        if (i == 0) {
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_INNER_JOIN) + RuleJsonConstant.LEFT_PARENTHESIS);
                        } else if (i != jsonArray.size() - 1) {
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_ON) + tableAliasPre + ".user_code" + RuleJsonConstant.addBlank(RuleJsonConstant.EQUAL) + tableAliasNow + ".user_code");
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_INNER_JOIN) + RuleJsonConstant.LEFT_PARENTHESIS);
                        } else {
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_ON) + tableAliasPre + ".user_code" + RuleJsonConstant.addBlank(RuleJsonConstant.EQUAL) + tableAliasNow + ".user_code");
                        }
                    } else {
                        if (i != jsonArray.size() - 1) {
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_UNION));
                        }
                    }
                } else if (level == 2 && jsonArray.size() > 1) {
                    if (RuleJsonConstant.RELATION_AND.equals(relation)) {
                        if (i == 0) {
                            sqlSb.append("select " + tableAliasNow + ".user_code from ");
                        }
                        sqlSb.append(RuleJsonConstant.LEFT_PARENTHESIS + "select user_code from event where ");
                    } else {
                        sqlSb.append("select user_code from event where ");
                    }

                    sqlSb.append(RuleJsonConstant.KEY_TIME_PARAMS + RuleJsonConstant.addBlank("between") + "'" +timeParamsArr.get(0) + "'" + RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_AND) + "'" + timeParamsArr.get(1) + "'");

                    //判断添加筛选只添加了一条还是多条
                    String filters = jsonObject.getString(RuleJsonConstant.KEY_FILTERS);//子规则
                    if (StringUtils.isNotBlank(filters) && !"[]".equals(filters)) {
                        sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_AND) + "user_code");
                        sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.IN) + RuleJsonConstant.LEFT_PARENTHESIS);
                        JSONArray filterArray = JSON.parseArray(filters);
                        JSONObject filterJsonObj = JSONObject.parseObject(filterArray.getString(0));
                        String filterType = filterJsonObj.getString(RuleJsonConstant.KEY_TYPE);
                        if (RuleJsonConstant.VALUE_FILTER.equals(filterType)) {//一条filter
                            sqlSb.append(filterService.getBaseFilterSql(filterJsonObj));
                        } else {//多条filter即含有subFilter
                            sqlSb.append(filterService.getBaseSubFilterSql(filterJsonObj));
                        }
                        sqlSb.append(RuleJsonConstant.RIGHT_PARENTHESIS);
                    }

                    //判断isDone, 是否做过
                    String isDone = jsonObject.getString(RuleJsonConstant.KEY_IS_DONE);
                    if (RuleJsonConstant.IS_DONE_FLAG.equals(isDone)) {
                        sqlSb.append(filterService.getMeasureInfoSql(jsonObject));
                    } else {
                        String measure = jsonObject.getString(RuleJsonConstant.KEY_MEASURE);
                        JSONObject measureJsonObj = JSONObject.parseObject(measure);
                        String eventName = measureJsonObj.getString(RuleJsonConstant.KEY_EVENT_NAME);
                        sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_AND) + RuleJsonConstant.KEY_EVENT_NAME + RuleJsonConstant.addBlank(RuleJsonConstant.EQUAL) + eventName);
                    }
                    if (RuleJsonConstant.RELATION_AND.equals(relation)) {
                        sqlSb.append(RuleJsonConstant.RIGHT_PARENTHESIS + RuleJsonConstant.BLANK + tableAliasNow);
                        if (i == 0) {
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_INNER_JOIN));
                        } else if (i != 0 && i != jsonArray.size() - 1) {
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_ON) + tableAliasPre + ".user_code" + RuleJsonConstant.addBlank(RuleJsonConstant.EQUAL) + tableAliasNow + ".user_code");
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_INNER_JOIN));
                        } else {
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_ON) + tableAliasPre + ".user_code" + RuleJsonConstant.addBlank(RuleJsonConstant.EQUAL) + tableAliasNow + ".user_code");
                        }
                    } else {
                        if (i != jsonArray.size() - 1) {
                            sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_UNION));
                        }
                    }
                } else {
                    sqlSb.append("select user_code from event where ");
                    sqlSb.append(RuleJsonConstant.KEY_TIME_PARAMS + RuleJsonConstant.addBlank("between") + timeParamsArr.get(0) + RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_AND) + timeParamsArr.get(1));

                    //判断添加筛选只添加了一条还是多条
                    String filters = jsonObject.getString(RuleJsonConstant.KEY_FILTERS);//子规则
                    JSONArray filterArray = JSON.parseArray(filters);
                    if (StringUtils.isNotBlank(filters) && filterArray.size() > 0) {
                        sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_AND) + "user_code");
                        sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.IN) + RuleJsonConstant.LEFT_PARENTHESIS);
                        //JSONArray filterArray = JSON.parseArray(filters);
                        JSONObject filterJsonObj = JSONObject.parseObject(filterArray.getString(0));
                        String filterType = filterJsonObj.getString(RuleJsonConstant.KEY_TYPE);
                        if (RuleJsonConstant.VALUE_FILTER.equals(filterType)) {//一条filter
                            sqlSb.append(filterService.getBaseFilterSql(filterJsonObj));
                        } else {//多条filter即含有subFilter
                            sqlSb.append(filterService.getBaseSubFilterSql(filterJsonObj));
                        }
                        sqlSb.append(RuleJsonConstant.RIGHT_PARENTHESIS);
                    }

                    //判断isDone, 是否做过
                    String isDone = jsonObject.getString(RuleJsonConstant.KEY_IS_DONE);
                    if (RuleJsonConstant.IS_DONE_FLAG.equals(isDone)) {
                        sqlSb.append(filterService.getMeasureInfoSql(jsonObject));
                    } else {
                        String measure = jsonObject.getString(RuleJsonConstant.KEY_MEASURE);
                        JSONObject measureJsonObj = JSONObject.parseObject(measure);
                        String eventName = measureJsonObj.getString(RuleJsonConstant.KEY_EVENT_NAME);
                        sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_AND) + RuleJsonConstant.KEY_EVENT_NAME + RuleJsonConstant.addBlank(RuleJsonConstant.EQUAL) + eventName);
                    }
                }
            }
        }
        filterNumber++;
        return sqlSb.toString();
    }
}