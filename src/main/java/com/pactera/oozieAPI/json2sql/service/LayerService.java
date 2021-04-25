package com.pactera.oozieAPI.json2sql.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pactera.oozieAPI.json2sql.contants.RuleJsonConstant;
import com.pactera.oozieAPI.json2sql.utils.StringUtils;

/**
 * @author SUN KI
 * @time 2021/4/21 14:27
 * @Desc 每一个分层
 */
public class LayerService {

    private EventRuleService eventRuleService = new EventRuleService();
    private ProfileRuleService profileRuleService = new ProfileRuleService();
    /**
     * 分层下规则layered
     *
     * @param rules
     * @return jsonRules
     */
    public JSONObject buildLayeredRuleSql(String rules) {
        JSONObject jsonObject = JSON.parseObject(rules);
        String relation = jsonObject.getString("relation");//关系
        JSONObject jsonRules = new JSONObject();
        StringBuilder result = new StringBuilder();
        if (jsonObject.containsKey("rules")) {//含有子规则
            String profileRuleResult;
            String eventRuleResult;
            String rulesChild = jsonObject.getString("rules");
            JSONArray rulesArray = JSON.parseArray(rulesChild);

            if (jsonObject.toJSONString().contains("profile_rule")) {//用户信息规则
                String profileJsonStr = "[" + JSON.toJSONString(rulesArray.get(0)) + "]";//一个分层数组只有两个元素, 第一个是profile_rule, 第二个是event_rule
                profileRuleResult = profileRuleService.bulidUserInfoRuleSql(profileJsonStr, relation, 1, 1);
                profileRuleService.resultSqlTemp.setLength(0);//如果有多个分层(比如分层一,分层二)的话, resultSqlTemp如果非空的话, 第二个分层就没有select那段sql了, 需要将resultSqlTemp清空之后, 才能保证每个分层第一个进去的添加select那段sql
                //jsonRules.put("profile_rule", result);
            } else {
                profileRuleResult = null;
            }
            if (jsonObject.toJSONString().contains("event_rule")) {//行为规则
                String eventJsonStr = "[" + JSON.toJSONString(rulesArray.get(1)) + "]";
                eventRuleResult = eventRuleService.buildEventRuleSql(eventJsonStr, relation, 1);
                //jsonRules.put("event_rule", result);
            } else {
                eventRuleResult = null;
            }
            if (StringUtils.isNotBlank(profileRuleResult) && StringUtils.isNotBlank(eventRuleResult)) {
                if (RuleJsonConstant.RELATION_AND.equals(relation)) {
                    result.append("select tt11.user_code from " + RuleJsonConstant.addParenthesis(profileRuleResult) + " tt11");
                    result.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_INNER_JOIN));
                    result.append(RuleJsonConstant.addParenthesis(eventRuleResult) + "tt12");
                    result.append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_ON) + "tt11.user_code = tt12.user_code");
                } else {
                    result.append(profileRuleResult).append(RuleJsonConstant.addBlank(RuleJsonConstant.RELATION_UNION)).append(eventRuleResult);
                }
            } else if (StringUtils.isNotBlank(profileRuleResult) && eventRuleResult == null) {
                result.append(profileRuleResult);
            } else if (StringUtils.isNotBlank(eventRuleResult) && profileRuleResult == null) {
                result.append(eventRuleResult);
            }
        }
        jsonRules.put("resultSql", result.toString());
        return jsonRules;
    }
}
