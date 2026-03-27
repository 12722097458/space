package com.ityj.service;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.ityj.config.FeishuConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 飞书多维表格 - 清空并写入留存率数据 Demo
 * 不依赖Spring，直接main方法运行
 */
public class FeishuBitableDemo {

    private static String APP_TOKEN = null;
    private static String TABLE_ID = null;
    // 飞书 user_access_token，过期后需要更新
    private static String FEISHU_TOKEN = null;

    private static String currentFeishuAuth = FEISHU_TOKEN;



    private static final String FEISHU_BASE_URL =
            "https://open.feishu.cn/open-apis/bitable/v1/apps/" + APP_TOKEN + "/tables/" + TABLE_ID;


    // 留存率数据接口
    private static final String RETENTION_API = "http://admin.web.pandaai.online/admin-api/user/users/retention/daily";
    private static final String RETENTION_TOKEN = "Bearer test1";
    private static final int FEISHU_BATCH_LIMIT = 500;
    private static final int FEISHU_PAGE_SIZE = 100;
    private static final Set<String> warnedMissingFields = new HashSet<>();

    public static void main(String[] args) {
        String startTime = "2026-03-20";
        String endTime = "2026-03-26";

        try {
            // 1. 查询所有recordId
            List<String> recordIds = searchAllRecordIds();
            System.out.println("查到历史记录 " + recordIds.size() + " 条");

            // 2. 批量删除
            if (!recordIds.isEmpty()) {
                batchDeleteRecords(recordIds);
                System.out.println("已清空表格历史数据");
            }

            // 3. 查询留存率数据
            List<JSONObject> retentionData = fetchRetentionData(startTime, endTime);
            System.out.println("查到留存率数据 " + retentionData.size() + " 条");

            // 4. 批量写入飞书多维表格
            if (!retentionData.isEmpty()) {
                Set<String> existingFieldNames = fetchExistingFieldNames();
                System.out.println("飞书表字段数: " + existingFieldNames.size());
                batchCreateRecords(retentionData, existingFieldNames);
                System.out.println("数据写入完成");
            } else {
                System.out.println("留存率数据为空，跳过写入");
            }
        } catch (Exception ex) {
            System.err.println("同步失败: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    /**
     * 查询所有recordId（分页）
     */
    private static List<String> searchAllRecordIds() {
        List<String> allRecordIds = new ArrayList<>();
        boolean hasMore = true;
        String pageToken = null;

        while (hasMore) {
            String url = FEISHU_BASE_URL + "/records/search?page_size=" + FEISHU_PAGE_SIZE;
            if (pageToken != null) {
                url += "&page_token=" + pageToken;
            }

            HttpResponse response = HttpRequest.post(url)
                    .header("Authorization", currentFeishuAuth)
                    .header("Content-Type", "application/json")
                    .body("{}")
                    .execute();
            response = retryIfTokenExpired(response, url, "{}");

            String resp = response.body();
            ensureHttpOk(response, "records/search");
            JSONObject result = JSONUtil.parseObj(resp);
            JSONObject data = ensureFeishuSuccess(result, "records/search");

            JSONArray items = data.getJSONArray("items");
            if (items != null) {
                for (int i = 0; i < items.size(); i++) {
                    String recordId = items.getJSONObject(i).getStr("record_id");
                    if (recordId != null && !recordId.isBlank()) {
                        allRecordIds.add(recordId);
                    }
                }
            }

            hasMore = data.getBool("has_more", false);
            pageToken = data.getStr("page_token");
        }
        return allRecordIds;
    }

    /**
     * 批量删除记录（每次最多500条）
     */
    private static void batchDeleteRecords(List<String> recordIds) {
        int batchSize = FEISHU_BATCH_LIMIT;
        for (int i = 0; i < recordIds.size(); i += batchSize) {
            List<String> batch = recordIds.subList(i, Math.min(i + batchSize, recordIds.size()));

            JSONObject body = new JSONObject();
            body.set("records", batch);

            HttpResponse response = HttpRequest.post(FEISHU_BASE_URL + "/records/batch_delete")
                    .header("Authorization", currentFeishuAuth)
                    .header("Content-Type", "application/json")
                    .body(body.toString())
                    .execute();
            response = retryIfTokenExpired(response, FEISHU_BASE_URL + "/records/batch_delete", body.toString());

            String resp = response.body();
            ensureHttpOk(response, "records/batch_delete");
            ensureFeishuSuccess(JSONUtil.parseObj(resp), "records/batch_delete");
            System.out.println("删除第 " + (i / batchSize + 1) + " 批成功, 条数=" + batch.size());
        }
    }

    /**
     * 查询留存率数据
     */
    private static List<JSONObject> fetchRetentionData(String startTime, String endTime) {
        String url = RETENTION_API + "?startTime=" + startTime + "&endTime=" + endTime;

        HttpResponse response = HttpRequest.get(url)
                .header("Authorization", RETENTION_TOKEN)
                .execute();

        String resp = response.body();
        ensureHttpOk(response, "retention/daily");
        JSONObject result = JSONUtil.parseObj(resp);
        int code = result.getInt("code", -1);
        if (code != 0) {
            throw new IllegalStateException("查询留存率失败, code=" + code + ", resp=" + resp);
        }

        JSONArray dataArray = result.getJSONArray("data");
        if (dataArray == null) {
            return Collections.emptyList();
        }
        List<JSONObject> list = new ArrayList<>();
        for (int i = 0; i < dataArray.size(); i++) {
            list.add(dataArray.getJSONObject(i));
        }
        return list;
    }

    /**
     * 批量写入飞书多维表格（每次最多500条）
     */
    private static void batchCreateRecords(List<JSONObject> dataList, Set<String> existingFieldNames) {
        int batchSize = FEISHU_BATCH_LIMIT;
        for (int i = 0; i < dataList.size(); i += batchSize) {
            List<JSONObject> batch = dataList.subList(i, Math.min(i + batchSize, dataList.size()));

            JSONArray records = new JSONArray();
            for (JSONObject item : batch) {
                JSONObject fields = buildFields(item, existingFieldNames);
                if (fields.isEmpty()) {
                    continue;
                }

                JSONObject record = new JSONObject();
                record.set("fields", fields);
                records.add(record);
            }
            if (records.isEmpty()) {
                System.out.println("第 " + (i / batchSize + 1) + " 批无可写入字段，跳过");
                continue;
            }

            JSONObject body = new JSONObject();
            body.set("records", records);

            HttpResponse response = HttpRequest.post(FEISHU_BASE_URL + "/records/batch_create")
                    .header("Authorization", currentFeishuAuth)
                    .header("Content-Type", "application/json")
                    .body(body.toString())
                    .execute();
            response = retryIfTokenExpired(response, FEISHU_BASE_URL + "/records/batch_create", body.toString());

            String resp = response.body();
            ensureHttpOk(response, "records/batch_create");
            ensureFeishuSuccess(JSONUtil.parseObj(resp), "records/batch_create");
            System.out.println("写入第 " + (i / batchSize + 1) + " 批成功, 条数=" + batch.size());
        }
    }

    private static JSONObject buildFields(JSONObject item, Set<String> existingFieldNames) {
        JSONObject fields = new JSONObject();
        putIfFieldExists(fields, existingFieldNames, "日期", item.getStr("date"));
        putIfFieldExists(fields, existingFieldNames, "注册数", item.getInt("registerCount", 0));
        putIfFieldExists(fields, existingFieldNames, "第1天", item.getStr("day1Rate", "-"));
        putIfFieldExists(fields, existingFieldNames, "第2天", item.getStr("day2Rate", "-"));
        putIfFieldExists(fields, existingFieldNames, "第3天", item.getStr("day3Rate", "-"));
        putIfFieldExists(fields, existingFieldNames, "第4天", item.getStr("day4Rate", "-"));
        putIfFieldExists(fields, existingFieldNames, "第5天", item.getStr("day5Rate", "-"));
        putIfFieldExists(fields, existingFieldNames, "第6天", item.getStr("day6Rate", "-"));
        putIfFieldExists(fields, existingFieldNames, "第7天", item.getStr("day7Rate", "-"));
        putIfFieldExists(fields, existingFieldNames, "第8天", item.getStr("day8Rate", "-"));
        return fields;
    }

    private static void putIfFieldExists(JSONObject fields, Set<String> existingFieldNames, String fieldName, Object value) {
        if (existingFieldNames.contains(fieldName)) {
            fields.set(fieldName, value);
            return;
        }
        if (warnedMissingFields.add(fieldName)) {
            System.out.println("飞书表缺少字段，已跳过写入: " + fieldName);
        }
    }

    private static Set<String> fetchExistingFieldNames() {
        Set<String> fieldNames = new HashSet<>();
        String pageToken = null;
        boolean hasMore = true;
        while (hasMore) {
            String url = FEISHU_BASE_URL + "/fields?page_size=" + FEISHU_PAGE_SIZE;
            if (pageToken != null) {
                url += "&page_token=" + pageToken;
            }
            HttpResponse response = HttpRequest.get(url)
                    .header("Authorization", currentFeishuAuth)
                    .execute();
            response = retryIfTokenExpiredGet(response, url);
            ensureHttpOk(response, "fields/list");
            JSONObject result = JSONUtil.parseObj(response.body());
            JSONObject data = ensureFeishuSuccess(result, "fields/list");

            JSONArray items = data.getJSONArray("items");
            if (items != null) {
                for (int i = 0; i < items.size(); i++) {
                    String fieldName = items.getJSONObject(i).getStr("field_name");
                    if (fieldName != null && !fieldName.isBlank()) {
                        fieldNames.add(fieldName);
                    }
                }
            }
            hasMore = data.getBool("has_more", false);
            pageToken = data.getStr("page_token");
        }
        return fieldNames;
    }

    private static void ensureHttpOk(HttpResponse response, String action) {
        if (response.getStatus() != 200) {
            throw new IllegalStateException(action + " http_status=" + response.getStatus() + ", body=" + response.body());
        }
    }

    private static HttpResponse retryIfTokenExpired(HttpResponse response, String url, String body) {
        if (response.getStatus() != 401) {
            return response;
        }
        if (!isTokenExpiredResponse(response.body())) {
            return response;
        }

        String newToken = fetchTenantAccessToken();
        currentFeishuAuth = "Bearer " + newToken;
        System.out.println("检测到飞书 token 过期，已切换为 tenant_access_token 重试请求");

        return HttpRequest.post(url)
                .header("Authorization", currentFeishuAuth)
                .header("Content-Type", "application/json")
                .body(body)
                .execute();
    }

    private static HttpResponse retryIfTokenExpiredGet(HttpResponse response, String url) {
        if (response.getStatus() != 401) {
            return response;
        }
        if (!isTokenExpiredResponse(response.body())) {
            return response;
        }

        String newToken = fetchTenantAccessToken();
        currentFeishuAuth = "Bearer " + newToken;
        System.out.println("检测到飞书 token 过期，已切换为 tenant_access_token 重试请求");

        return HttpRequest.get(url)
                .header("Authorization", currentFeishuAuth)
                .execute();
    }

    private static boolean isTokenExpiredResponse(String body) {
        try {
            JSONObject json = JSONUtil.parseObj(body);
            int code = json.getInt("code", -1);
            return code == 99991677;
        } catch (Exception ignore) {
            return false;
        }
    }

    private static String fetchTenantAccessToken() {
        JSONObject req = new JSONObject();
        req.set("app_id", FeishuConfig.APP_ID);
        req.set("app_secret", FeishuConfig.APP_SECRET);

        HttpResponse response = HttpRequest.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal")
                .header("Content-Type", "application/json")
                .body(req.toString())
                .execute();
        ensureHttpOk(response, "auth/tenant_access_token/internal");

        JSONObject json = JSONUtil.parseObj(response.body());
        int code = json.getInt("code", -1);
        if (code != 0) {
            throw new IllegalStateException("获取tenant_access_token失败, code=" + code + ", resp=" + response.body());
        }
        String token = json.getStr("tenant_access_token");
        if (token == null || token.isBlank()) {
            throw new IllegalStateException("获取tenant_access_token失败, 响应中无tenant_access_token");
        }
        return token;
    }

    private static JSONObject ensureFeishuSuccess(JSONObject response, String action) {
        int code = response.getInt("code", -1);
        if (code != 0) {
            throw new IllegalStateException(action + " feishu_code=" + code + ", msg=" + response.getStr("msg") + ", resp=" + response);
        }
        JSONObject data = response.getJSONObject("data");
        if (data == null) {
            throw new IllegalStateException(action + " 响应缺少data: " + response);
        }
        return data;
    }
}
