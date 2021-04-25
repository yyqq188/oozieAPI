package com.pactera.oozieAPI.json2sql.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pactera.oozieAPI.json2sql.contants.RuleJsonConstant;
import com.pactera.oozieAPI.json2sql.utils.StringUtils;

/**
 * @author SUN KI
 * @time 2021/4/13 9:08
 * @Desc 过滤部分
 */

public class FilterService {

    //有filter且不存在subFilter时进入该方法
    public String getBaseFilterSql(JSONObject jsonObject) {
        StringBuilder sqlSb = new StringBuilder();
        String filterFiled = jsonObject.getString(RuleJsonConstant.KEY_FIELD);
        String filterFunction = jsonObject.getString(RuleJsonConstant.KEY_FUNCTION);//方法，方法，most（小于等于），least（大于等于），greater（大于），equal（等于），这里我们不参照，太扯淡了，就用传统的
        String filterParams = jsonObject.getString(RuleJsonConstant.KEY_PARAMS);//参数
        String tableName = "";
        String columnName = "";

        if (StringUtils.isNotBlank(filterFiled) && filterFiled.contains(".")) {
            String[] fieldArr = filterFiled.split("\\.");
            if (fieldArr.length == 2) {
                tableName = fieldArr[0];
                columnName = fieldArr[1];
            }
            if (fieldArr.length == 3) {
                tableName = fieldArr[0];
                columnName = fieldArr[2];
            }
            sqlSb.append("select user_code from " + tableName + RuleJsonConstant.addBlank(RuleJsonConstant.TABLE_ALIAS_TF) + "where ");
            JSONArray paramsArr = JSON.parseArray(filterParams);
            String paramsStr = StringUtils.join(paramsArr, ",");
            //根据方法类型,拼接过滤条件
            if (paramsArr != null) {
                switch (filterFunction) {
                    case "eq":
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + "= '" + paramsArr.get(0) + "'");
                        break;
                    case "uneq":
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " != '" + paramsArr.get(0) + "'");
                        break;
                    case "in":
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " in (" + paramsStr + ")");
                        break;
                    case "unin":
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " not in (" + paramsStr + ")");
                        break;
                    case "has"://有值
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " is not null");
                        break;
                    case "unhas":
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " is null");
                        break;
                    case "blank"://为null
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " is null");
                        break;
                    case "unblank"://不为null
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " is not null");
                        break;
                    case "like"://模糊匹配
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " like '" + paramsStr + "'");
                        break;
                    case "unlike":
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " not like '" + paramsStr + "'");
                        break;
                    case "lt"://小于
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " < " + paramsArr.get(0));
                        break;
                    case "lte"://小于等于
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " <= " + paramsArr.get(0));
                        break;
                    case "gt"://大于
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " > " + paramsArr.get(0));
                        break;
                    case "gte"://大于等于
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " >= " + paramsArr.get(0));
                        break;
                    case "between"://范围
                        if (paramsArr.get(0) instanceof String) {
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " between '" + paramsArr.get(0) + "' and '" + paramsArr.get(1) + "'");
                        } else {
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " between " + paramsArr.get(0) + " and " + paramsArr.get(1));
                        }
                        break;
                    default:
                        sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + "='" + paramsArr.get(0) + "'");
                        break;
                }
            }
        }

        return sqlSb.toString();
    }

    //存在subFilter时, 进入到该方法
    public String getBaseSubFilterSql(JSONObject jsonObject) {
        StringBuilder sqlSb = new StringBuilder();
        String subFilter = jsonObject.getString(RuleJsonConstant.KEY_SUB_FILTER);
        JSONArray subFilterArr = JSON.parseArray(subFilter);
        for (int i = 0; i < subFilterArr.size(); i++) {
            JSONObject subFilterJsonObj = JSONObject.parseObject(subFilterArr.getString(i));
            String subFilterField = subFilterJsonObj.getString(RuleJsonConstant.KEY_FIELD);
            String subFilterFunction = subFilterJsonObj.getString(RuleJsonConstant.KEY_FUNCTION);
            String subFilterParams = subFilterJsonObj.getString(RuleJsonConstant.KEY_PARAMS);

            if (StringUtils.isNotBlank(subFilterField) && subFilterField.contains(".")) {
                String tableName = "";
                String columnName = "";
                String[] subFieldArr = subFilterField.split("\\.");
                if (subFieldArr.length == 2) {
                    tableName = subFieldArr[0];
                    columnName = subFieldArr[1];
                }
                if (subFieldArr.length == 3) {
                    tableName = subFieldArr[0];
                    columnName = subFieldArr[2];
                }

                if (i == 0){
                    sqlSb.append("select user_code from " + tableName + RuleJsonConstant.addBlank(RuleJsonConstant.TABLE_ALIAS_TF) + "where ");
                }

                JSONArray paramsArr = JSON.parseArray(subFilterParams);
                String paramsStr = StringUtils.join(paramsArr, ",");
                //根据方法类型,拼接过滤条件
                if (paramsArr != null) {
                    switch (subFilterFunction) {
                        case "eq":
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + "= '" + paramsArr.get(0) + "'");
                            break;
                        case "uneq":
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " != '" + paramsArr.get(0) + "'");
                            break;
                        case "in":
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " in (" + paramsStr + ")");
                            break;
                        case "unin":
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " not in (" + paramsStr + ")");
                            break;
                        case "has"://有值
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " is not null");
                            break;
                        case "unhas":
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " is null");
                            break;
                        case "blank"://为null
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " is null");
                            break;
                        case "unblank"://不为null
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " is not null");
                            break;
                        case "like"://模糊匹配
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " like '" + paramsStr + "'");
                            break;
                        case "unlike":
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " not like '" + paramsStr + "'");
                            break;
                        case "lt"://小于
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " < " + paramsArr.get(0));
                            break;
                        case "lte"://小于等于
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " <= " + paramsArr.get(0));
                            break;
                        case "gt"://大于
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " > " + paramsArr.get(0));
                            break;
                        case "gte"://大于等于
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " >= " + paramsArr.get(0));
                            break;
                        case "between"://范围
                            if (paramsArr.get(0) instanceof String) {
                                sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " between '" + paramsArr.get(0) + "' and '" + paramsArr.get(1) + "'");
                            } else {
                                sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " between " + paramsArr.get(0) + " and " + paramsArr.get(1));
                            }
                            break;
                        default:
                            sqlSb.append(RuleJsonConstant.TABLE_ALIAS_TF + "." + columnName + " = '" + paramsArr.get(0) + "'");
                            break;
                    }
                }
                if (i != subFilterArr.size() - 1) {
                    sqlSb.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_AND));
                }
            }
        }
        return sqlSb.toString();
    }

    public String getBaseRuleSql(JSONObject jsonObject) {
        String childFiled = jsonObject.getString("field");//字段
        String childFunction = jsonObject.getString("function");//方法，方法，most（小于等于），least（大于等于），greater（大于），equal（等于），这里我们不参照，太扯淡了，就用传统的
        String childParams = jsonObject.getString("params");//参数
        String columnName = "";
        StringBuffer sqlSb = new StringBuffer();
        if (childFiled != null && childFiled.contains(".")) {
            String[] fieldArr = childFiled.split("\\.");
            columnName = fieldArr[1];

            JSONArray paramsArr = JSON.parseArray(childParams);
            String paramsStr = StringUtils.join(paramsArr, ",");

            //根据方法类型,拼接过滤条件
            if (paramsArr != null && paramsArr.size() > 0) {
                switch (childFunction) {
                    case "eq":
                        sqlSb.append(columnName + " = '" + paramsArr.get(0) + "'");
                        break;
                    case "uneq":
                        sqlSb.append(columnName + " != '" + paramsArr.get(0) + "'");
                        break;
                    case "in":
                        sqlSb.append(columnName + " in (" + paramsStr + ")");
                        break;
                    case "unin":
                        sqlSb.append(columnName + " not in (" + paramsStr + ")");
                        break;
                    case "has"://有值
                        sqlSb.append(columnName + " is not null");
                        break;
                    case "unhas":
                        sqlSb.append(columnName + " is null");
                        break;
                    case "blank"://为null
                        sqlSb.append(columnName + " is null");
                        break;
                    case "unblank"://不为null
                        sqlSb.append(columnName + " is not null");
                        break;
                    case "like"://模糊匹配
                        sqlSb.append(columnName + " like '" + paramsStr + "'");
                        break;
                    case "unlike":
                        sqlSb.append(columnName + " not like '" + paramsStr + "'");
                        break;
                    case "lt"://小于
                        sqlSb.append(columnName + " < " + paramsArr.get(0));
                        break;
                    case "lte"://小于等于
                        sqlSb.append(columnName + " <= " + paramsArr.get(0));
                        break;
                    case "gt"://大于
                        sqlSb.append(columnName + " > " + paramsArr.get(0));
                        break;
                    case "gte"://大于等于
                        sqlSb.append(columnName + " >= " + paramsArr.get(0));
                        break;
                    case "between"://范围
                        if (paramsArr.get(0) instanceof String) {
                            sqlSb.append(columnName + " between '" + paramsArr.get(0) + "' and '" + paramsArr.get(1) + "'");
                        } else {
                            sqlSb.append(columnName + " between " + paramsArr.get(0) + " and " + paramsArr.get(1));
                        }
                        break;
                    default:
                        sqlSb.append(columnName + " = '" + paramsArr.get(0) + "'");
                        break;
                }
            }
        }
        return sqlSb.toString();
    }

    public String getMeasureInfoSql(JSONObject jsonObject) {

        StringBuilder sqlSb = new StringBuilder();

        String measureFunction = jsonObject.getString(RuleJsonConstant.KEY_FUNCTION);
        String measureParams = jsonObject.getString(RuleJsonConstant.KEY_PARAMS);
        String measure = jsonObject.getString(RuleJsonConstant.KEY_MEASURE);
        JSONObject measureJsonObj = JSONObject.parseObject(measure);
        String eventName = measureJsonObj.getString(RuleJsonConstant.KEY_EVENT_NAME);
        sqlSb.append(" and " + RuleJsonConstant.KEY_EVENT_NAME + " = " + "'" + eventName + "'");
        String aggregator = measureJsonObj.getString(RuleJsonConstant.KEY_AGGREGATOR);
        String measureField = measureJsonObj.getString(RuleJsonConstant.KEY_FIELD);
        String columnName = "";
            String[] measureFieldArr = measureField.split("\\.");
            if (measureFieldArr.length == 2) {
                columnName = measureFieldArr[1];
            } else if (measureFieldArr.length == 3) {
                columnName = measureFieldArr[2];
            }else {
                columnName = "1";
            }
            sqlSb.append(" group by user_code having " + aggregator + RuleJsonConstant.addParenthesis(columnName));
            JSONArray paramsArr = JSON.parseArray(measureParams);
            String paramsStr = StringUtils.join(paramsArr, ",");
            //根据方法类型,拼接过滤条件
            if (paramsArr != null && paramsArr.size() > 0) {
                switch (measureFunction) {
                    case "eq":
                        sqlSb.append(" = '" + paramsArr.get(0) + "'");
                        break;
                    case "uneq":
                        sqlSb.append(" != '" + paramsArr.get(0) + "'");
                        break;
                    case "in":
                        sqlSb.append(" in (" + paramsStr + ")");
                        break;
                    case "unin":
                        sqlSb.append(" not in (" + paramsStr + ")");
                        break;
                    case "has"://有值
                        sqlSb.append(" is not null");
                        break;
                    case "unhas":
                        sqlSb.append(" is null");
                        break;
                    case "blank"://为null
                        sqlSb.append(" is null");
                        break;
                    case "unblank"://不为null
                        sqlSb.append(" is not null");
                        break;
                    case "like"://模糊匹配
                        sqlSb.append(" like '" + paramsStr + "'");
                        break;
                    case "unlike":
                        sqlSb.append(" not like '" + paramsStr + "'");
                        break;
                    case "lt"://小于
                        sqlSb.append(" < " + paramsArr.get(0));
                        break;
                    case "lte"://小于等于
                        sqlSb.append(" <= " + paramsArr.get(0));
                        break;
                    case "gt"://大于
                        sqlSb.append(" > " + paramsArr.get(0));
                        break;
                    case "gte"://大于等于
                        sqlSb.append(" >= " + paramsArr.get(0));
                        break;
                    case "between"://范围
                        if (paramsArr.get(0) instanceof String) {
                            sqlSb.append(" between '" + paramsArr.get(0) + "' and '" + paramsArr.get(1) + "'");
                        } else {
                            sqlSb.append(" between " + paramsArr.get(0) + " and " + paramsArr.get(1));
                        }
                        break;
                    default:
                        sqlSb.append(" = '" + paramsArr.get(0) + "'");
                        break;
                }
            }
        return sqlSb.toString();
    }
}
