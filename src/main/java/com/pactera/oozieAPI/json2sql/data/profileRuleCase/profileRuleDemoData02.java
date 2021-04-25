package com.pactera.oozieAPI.json2sql.data.profileRuleCase;

/**
 * @author SUN KI
 * @time 2021/4/14 16:41
 * @Desc 共三层, 两个第二层, 一个第三层, 先进第三层的情况
 */
public class profileRuleDemoData02 {
    public static String getProfileRule_2() {
        return "{\n" +
                "  \"name\": \"\",\n" +
                "  \"cname\": \"标签新建\",\n" +
                "  \"dateRange\": [],\n" +
                "  \"region\": \"1\",\n" +
                "  \"resource\": \"1\",\n" +
                "  \"editableTabsValue\": 1,\n" +
                "  \"ruleContentList\": [\n" +
                "    {\n" +
                "      \"key\": 1,\n" +
                "      \"comment\": \"\",\n" +
                "      \"value\": \"分层1\",\n" +
                "      \"type\": \"rules_relation\",\n" +
                "      \"relation\": \"and\",\n" +
                "      \"closable\": false,\n" +
                "      \"rules\": [\n" +
                "        {\n" +
                "          \"type\": \"rules_relation\",\n" +
                "          \"relation\": \"or\",\n" +
                "          \"rules\": [\n" +
                "            {\n" +
                "              \"type\": \"rules_relation\",\n" +
                "              \"relation\": \"and\",\n" +
                "              \"rules\": [\n" +
                "                {\n" +
                "                  \"type\": \"profile_rule\",\n" +
                "                  \"field\": \"user.$utm_matching_type\",\n" +
                "                  \"function\": \"eq\",\n" +
                "                  \"params\": [\"1\"]\n" +
                "                },\n" +
                "                {\n" +
                "                  \"type\": \"profile_rule\",\n" +
                "                  \"field\": \"user.$utm_matching_type\",\n" +
                "                  \"function\": \"eq\",\n" +
                "                  \"params\": [\"2\"]\n" +
                "                }\n" +
                "              ]\n" +
                "            },\n" +
                "            {\n" +
                "              \"type\": \"profile_rule\",\n" +
                "              \"field\": \"user.$utm_matching_type\",\n" +
                "              \"function\": \"between\",\n" +
                "              \"selectTwo\": \"\",\n" +
                "              \"selectTwoData\": \"\",\n" +
                "              \"params\": [\n" +
                "                \"1\",\n" +
                "                \"2\"\n" +
                "              ]\n" +
                "            },\n" +
                "            {\n" +
                "              \"type\": \"profile_rule\",\n" +
                "              \"field\": \"user.clickTimes\",\n" +
                "              \"function\": \"gt\",\n" +
                "              \"params\": [\"5\"]\n" +
                "            }\n" +
                "          ]\n" +
                "        },\n" +
                "        {\n" +
                "          \"type\": \"rules_relation\",\n" +
                "          \"relation\": \"and\",\n" +
                "          \"rules\": [{\n" +
                "            \"type\": \"rules_relation\",\n" +
                "            \"relation\": \"and\",\n" +
                "            \"rules\": [\n" +
                "              {\n" +
                "                \"measure\": {\n" +
                "                  \"aggregator\": \"general\",\n" +
                "                  \"field\": \"\",\n" +
                "                  \"type\": \"event_measure\",\n" +
                "                  \"event_name\": \"$AppViewScreen\"\n" +
                "                },\n" +
                "                \"type\": \"event_rule\",\n" +
                "                \"time_function\": \"absolute_time\",\n" +
                "                \"time_params\": [\n" +
                "                  \"2021-03-05\",\n" +
                "                  \"2021-03-05\"\n" +
                "                ],\n" +
                "                \"params\": [\n" +
                "                  11\n" +
                "                ],\n" +
                "                \"function\": \"least\",\n" +
                "                \"isdone\": \"1\",\n" +
                "                \"filters\": [\n" +
                "                  {\n" +
                "                    \"type\": \"filter\",\n" +
                "                    \"field\": \"event.AppCrashed.$ip\",\n" +
                "                    \"function\": \"equal\",\n" +
                "                    \"params\": [\n" +
                "                      \"22222\"\n" +
                "                    ]\n" +
                "                  }\n" +
                "                ]\n" +
                "              },\n" +
                "              {\n" +
                "                \"measure\": {\n" +
                "                  \"aggregator\": \"general\",\n" +
                "                  \"field\": \"\",\n" +
                "                  \"type\": \"event_measure\",\n" +
                "                  \"event_name\": \"$AppViewScreen\"\n" +
                "                },\n" +
                "                \"type\": \"event_rule\",\n" +
                "                \"time_function\": \"absolute_time\",\n" +
                "                \"time_params\": [\n" +
                "                  \"2021-03-01\",\n" +
                "                  \"2021-03-02\"\n" +
                "                ],\n" +
                "                \"params\": [\n" +
                "                  22\n" +
                "                ],\n" +
                "                \"function\": \"least\",\n" +
                "                \"isdone\": \"1\",\n" +
                "                \"filters\": [\n" +
                "                  {\n" +
                "                    \"type\": \"filters_relation\",\n" +
                "                    \"relation\": \"and\",\n" +
                "                    \"subfilters\": [\n" +
                "                      {\n" +
                "                        \"field\": \"event.$AppClick.$distinct_id\",\n" +
                "                        \"function\": \"equal\",\n" +
                "                        \"params\": [\n" +
                "                          \"11\"\n" +
                "                        ],\n" +
                "                        \"type\": \"filter\"\n" +
                "                      },\n" +
                "                      {\n" +
                "                        \"field\": \"event.$AppClick.$distinct_id\",\n" +
                "                        \"function\": \"equal\",\n" +
                "                        \"params\": [\n" +
                "                          \"2\"\n" +
                "                        ],\n" +
                "                        \"type\": \"filter\"\n" +
                "                      }\n" +
                "                    ]\n" +
                "                  }\n" +
                "                ]\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "            {\n" +
                "              \"measure\": {\n" +
                "                \"aggregator\": \"general\",\n" +
                "                \"field\": \"\",\n" +
                "                \"type\": \"event_measure\",\n" +
                "                \"event_name\": \"$AppViewScreen\"\n" +
                "              },\n" +
                "              \"type\": \"event_rule\",\n" +
                "              \"time_function\": \"absolute_time\",\n" +
                "              \"time_params\": [\n" +
                "                \"2021-03-05\",\n" +
                "                \"2021-03-05\"\n" +
                "              ],\n" +
                "              \"params\": [\n" +
                "                11\n" +
                "              ],\n" +
                "              \"function\": \"least\",\n" +
                "              \"isdone\": \"1\",\n" +
                "              \"filters\": [\n" +
                "                {\n" +
                "                \"type\": \"filter\",\n" +
                "                \"field\": \"event.$AppStart.$event_duration\",\n" +
                "                \"function\": \"greater\",\n" +
                "                \"params\": [\"8\"]\n" +
                "                }\n" +
                "              ]\n" +
                "            },\n" +
                "            {\n" +
                "              \"measure\": {\n" +
                "                \"aggregator\": \"general\",\n" +
                "                \"field\": \"\",\n" +
                "                \"type\": \"event_measure\",\n" +
                "                \"event_name\": \"$AppViewScreen\"\n" +
                "              },\n" +
                "              \"type\": \"event_rule\",\n" +
                "              \"time_function\": \"absolute_time\",\n" +
                "              \"time_params\": [\n" +
                "                \"2021-03-01\",\n" +
                "                \"2021-03-02\"\n" +
                "              ],\n" +
                "              \"params\": [\n" +
                "                22\n" +
                "              ],\n" +
                "              \"function\": \"least\",\n" +
                "              \"isdone\": \"1\",\n" +
                "              \"filters\": [\n" +
                "                {\n" +
                "                  \"type\": \"filters_relation\",\n" +
                "                  \"relation\": \"and\",\n" +
                "                  \"subfilters\": [\n" +
                "                    {\n" +
                "                      \"field\": \"event.$AppClick.$distinct_id\",\n" +
                "                      \"function\": \"equal\",\n" +
                "                      \"params\": [\n" +
                "                        \"11\"\n" +
                "                      ],\n" +
                "                      \"type\": \"filter\"\n" +
                "                    },\n" +
                "                    {\n" +
                "                      \"field\": \"event.$AppClick.$distinct_id\",\n" +
                "                      \"function\": \"equal\",\n" +
                "                      \"params\": [\n" +
                "                        \"2\"\n" +
                "                      ],\n" +
                "                      \"type\": \"filter\"\n" +
                "                    }\n" +
                "                  ]\n" +
                "                }\n" +
                "              ]\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";
    }
}
