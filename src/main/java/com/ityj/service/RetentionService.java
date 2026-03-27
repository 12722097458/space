package com.ityj.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ityj.config.FeishuConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class RetentionService {

    @Autowired
    private RestTemplate restTemplate;

    public void sync() {

        // 1. 拉数据（改成你自己的）
        String url = "http://admin.web.pandaai.online/admin-api/user/users/retention/daily?startTime=2026-03-18&endTime=2026-03-24";

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer test1");

        HttpEntity<String> entity = new HttpEntity<>(headers);

        String body = restTemplate.exchange(
                url, HttpMethod.GET, entity, String.class
        ).getBody();

        JSONObject json = JSON.parseObject(body);
        JSONArray data = json.getJSONArray("data");

        List<Map> list = data.toJavaList(Map.class);
        // 2. 获取飞书 token
        String token = getTenantToken();

        // 3. 清空表
        clearTable(token);

        // 4. 写入
        insertBatch(token, list);
    }

    // ================== 飞书部分 ==================

    private String getTenantToken() {

        String url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal";

        Map<String, String> body = new HashMap<>();
        body.put("app_id", FeishuConfig.APP_ID);
        body.put("app_secret", FeishuConfig.APP_SECRET);

        String res = restTemplate.postForObject(url, body, String.class);

        return JSON.parseObject(res).getString("tenant_access_token");
    }

    private void clearTable(String token) {

        List<String> ids = getAllRecordIds(token);
        if (ids.isEmpty()) return;

        List<Map<String, String>> records = new ArrayList<>();
        for (String id : ids) {
            records.add(Map.of("record_id", id));
        }

        Map<String, Object> body = new HashMap<>();
        body.put("records", records);

        post("/records/batch_delete", token, body);
    }

    private List<String> getAllRecordIds(String token) {

        String url = FeishuConfig.BASE_URL + "/records";

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + token);

        HttpEntity<Void> entity = new HttpEntity<>(headers);

        String res = restTemplate.exchange(
                url, HttpMethod.GET, entity, String.class
        ).getBody();

        List<String> ids = new ArrayList<>();

        JSONArray items = JSON.parseObject(res)
                .getJSONObject("data")
                .getJSONArray("items");

        for (int i = 0; i < items.size(); i++) {
            ids.add(items.getJSONObject(i).getString("record_id"));
        }

        return ids;
    }

    private void insertBatch(String token, List<Map> list) {

        List<Map<String, Object>> batch = new ArrayList<>();

        for (Map<String, Object> item : list) {

            Map<String, Object> fields = new HashMap<>();
            fields.put("日期", item.get("date"));
            fields.put("注册数", item.get("registerCount"));
            fields.put("第1天", item.get("day1"));
            fields.put("第2天", item.get("day2"));
            fields.put("第3天", item.get("day3"));
            fields.put("第4天", item.get("day4"));
            fields.put("第5天", item.get("day5"));
            fields.put("第6天", item.get("day6"));
            fields.put("第7天", item.get("day7"));
            fields.put("第8天", item.get("day8"));

            batch.add(Map.of("fields", fields));

            if (batch.size() == 500) {
                sendBatch(token, batch);
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            sendBatch(token, batch);
        }
    }

    private void sendBatch(String token, List<Map<String, Object>> batch) {

        Map<String, Object> body = new HashMap<>();
        body.put("records", batch);

        post("/records/batch_create", token, body);
    }

    private void post(String path, String token, Object body) {

        String url = FeishuConfig.BASE_URL + path;

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + token);
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Object> entity = new HttpEntity<>(body, headers);

        restTemplate.postForObject(url, entity, String.class);
    }
}