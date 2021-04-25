package com.pactera.oozieAPI.json2sql.contants;

import com.pactera.oozieAPI.json2sql.utils.StringUtils;
import com.sun.javafx.logging.PulseLogger;
import org.apache.http.conn.util.PublicSuffixList;

/**
 * json解析常量类
 */
public class RuleJsonConstant {

    /**
     * JSON串 KEYS
     */
    public static final String VALUE_RULE_RELATION = "rules_relation";
    public static final String VALUE_FILTER_RELATION = "filters_relation";
    public static final String VALUE_EVENT_RULE = "event_rule";
    public static final String VALUE_PROFILE_RULE = "profile_rule";
    public static final String VALUE_FILTER = "filter";
    public static final String KEY_TYPE = "type";
    public static final String KEY_RELATION = "relation";
    public static final String KEY_RULES = "rules";
    public static final String KEY_FILTERS = "filters";
    public static final String KEY_FIELD = "field";
    public static final String KEY_FUNCTION = "function";
    public static final String KEY_PARAMS = "params";
    public static final String KEY_SUB_FILTER = "subfilters";
    public static final String KEY_IS_DONE = "isdone";
    public static final String KEY_MEASURE = "measure";
    public static final String KEY_EVENT_NAME = "event_name";
    public static final String KEY_AGGREGATOR = "aggregator";
    public static final String KEY_TIME_PARAMS = "time_params";
    /**
     * Filters's relations
     */
    public static final String RELATION_AND = "and";
    public static final String RELATION_INNER_JOIN = "inner join";
    public static final String RELATION_UNION = "union";
    public static final String RELATION_ON = "on";
    /**
     * logic symbol
     */
    public static final String EQUAL = "=";
    public static final String IN = "in";
    public static final String BLANK = " ";
    /**
     * tableAlias
     */
    public static final String TABLE_ALIAS_TF = "tf";
    /**
     * data dictionary
     */
    public static final String IS_DONE_FLAG = "1";
    /**
     * sign
     */
    public static final String LEFT_PARENTHESIS = "(";
    public static final String RIGHT_PARENTHESIS = ")";

    public static String addBlank(String param) {
        return " " + param + " ";
    }
    public static String addParenthesis(String param) {
        return LEFT_PARENTHESIS + param + RIGHT_PARENTHESIS;
    }
}
