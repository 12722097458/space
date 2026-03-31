package com.ityj.feishu.bitable.sync;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.ityj.feishu.bitable.api.FeishuBitableApiPaths;
import com.ityj.feishu.bitable.client.RetentionApiClient;
import com.ityj.feishu.bitable.config.FeishuTableProfile;
import com.ityj.feishu.bitable.config.FeishuBitableProperties;
import com.ityj.feishu.bitable.config.RetentionApiSettings;
import com.ityj.feishu.bitable.mapping.ColumnBinding;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 飞书多维表格：支持多 {@code table_id} 与不同数据源、列映射（表头），以及预组装行数据写入。
 */
@Slf4j
@Service
public class FeishuBitableSyncService {

    private final FeishuBitableProperties props;
    private final RetentionApiClient retentionApiClient;

    private String currentFeishuAuth;
    private long tenantTokenExpiresAtEpochMs;

    public FeishuBitableSyncService(FeishuBitableProperties props, RetentionApiClient retentionApiClient) {
        this.props = props;
        this.retentionApiClient = retentionApiClient;
    }

    /**
     * @param profileKey {@code feishu.bitable.tables} 中的 key（如 {@code retention_daily}）
     */
    public void sync(String profileKey, String startTime, String endTime) {
        FeishuTableProfile profile = resolveProfile(profileKey);
        String base = props.bitableBaseUrlForTable(profile.getTableId());
        ensureTenantToken();
        Set<String> warned = new HashSet<>();

        List<String> recordIds = searchAllRecordIds(base);
        System.out.println("[" + profileKey + "] 查到历史记录 " + recordIds.size() + " 条");

        if (!recordIds.isEmpty()) {
            batchDeleteRecords(base, recordIds);
            System.out.println("[" + profileKey + "] 已清空表格历史数据");
        }
        RetentionApiSettings retentionApiSettings = props.getRetention();
        List<JSONObject> retentionData = retentionApiClient.fetchDailyRows(retentionApiSettings, startTime, endTime);
        System.out.println("[" + profileKey + "] 查到业务数据 " + retentionData.size() + " 条");

        if (retentionData.isEmpty()) {
            System.out.println("[" + profileKey + "] 数据为空，跳过写入");
            return;
        }

        Set<String> existingFieldNames = fetchExistingFieldNames(base);
        System.out.println("[" + profileKey + "] 飞书表字段数: " + existingFieldNames.size());

        List<ColumnBinding> bindings = profile.getColumns();
        if (bindings != null && !bindings.isEmpty()) {
            batchCreateRecords(base, retentionData, existingFieldNames, bindings, warned);
        } else {
            batchCreateRecordsLegacy(base, retentionData, existingFieldNames, warned);
        }
        System.out.println("[" + profileKey + "] 数据写入完成");
    }

    /**
     * 在表末尾新增一条记录（飞书 OpenAPI：POST .../records，与官方 Java SDK 的 create 记录一致）。
     * <p>
     * 仅写入当前表中已存在的字段名；不存在的列名会跳过并打日志。
     *
     * @return 新增行的 {@code record_id}
     */
    public String appendRecord(String profileKey, Map<String, Object> fields) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("fields 不能为空");
        }
        FeishuTableProfile profile = resolveProfile(profileKey);
        String base = props.bitableBaseUrlForTable(profile.getTableId());
        ensureTenantToken();

        Set<String> existingFieldNames = fetchExistingFieldNames(base);
        Set<String> warned = new HashSet<>();
        JSONObject feishuFields = new JSONObject();
        for (Map.Entry<String, Object> e : fields.entrySet()) {
            putIfFieldExists(feishuFields, existingFieldNames, e.getKey(), e.getValue(), warned);
        }
        if (feishuFields.isEmpty()) {
            throw new IllegalArgumentException("没有可写入的字段，请核对列名是否与多维表格中的字段名称完全一致");
        }

        JSONObject body = new JSONObject();
        body.set("fields", feishuFields);
        String path = base + FeishuBitableApiPaths.RECORDS;
        HttpResponse response = HttpRequest.post(path)
                .header("Authorization", currentFeishuAuth)
                .header("Content-Type", "application/json; charset=utf-8")
                .body(body.toString())
                .execute();
        response = retryIfTokenExpired(response, path, body.toString());

        ensureHttpOk(response, "records/create");
        JSONObject result = JSONUtil.parseObj(response.body());
        JSONObject data = ensureFeishuSuccess(result, "records/create");
        JSONObject record = data.getJSONObject("record");
        if (record == null) {
            throw new IllegalStateException("records/create 响应缺少 record: " + response.body());
        }
        String recordId = record.getStr("record_id");
        if (recordId == null || recordId.isBlank()) {
            recordId = record.getStr("id");
        }
        if (recordId == null || recordId.isBlank()) {
            throw new IllegalStateException("records/create 响应缺少 record_id: " + response.body());
        }
        log.info("[{}] 新增记录成功 recordId={}", profileKey, recordId);
        return recordId;
    }

    private FeishuTableProfile resolveProfile(String profileKey) {
        Map<String, FeishuTableProfile> tables = props.getTables();
        if (tables != null && tables.containsKey(profileKey)) {
            FeishuTableProfile p = tables.get(profileKey);
            if (p.getTableId() == null || p.getTableId().isBlank()) {
                throw new IllegalStateException("feishu.bitable.tables." + profileKey + ".table-id 未配置");
            }
            return p;
        }
        throw new IllegalArgumentException("Unknown table profile: " + profileKey);
    }

    private void ensureTenantToken() {
        long now = System.currentTimeMillis();
        if (currentFeishuAuth != null
                && !currentFeishuAuth.isBlank()
                && now < tenantTokenExpiresAtEpochMs - 60_000L) {
            return;
        }
        refreshTenantAccessTokenInternal();
    }

    private void refreshTenantAccessTokenInternal() {
        JSONObject req = new JSONObject();
        req.set("app_id", props.getAppId());
        req.set("app_secret", props.getAppSecret());

        HttpResponse response = HttpRequest.post(props.getTenantAccessTokenUrl())
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
        int expireSec = json.getInt("expire", 7200);
        tenantTokenExpiresAtEpochMs = System.currentTimeMillis() + expireSec * 1000L;
        currentFeishuAuth = "Bearer " + token;
    }

    private List<String> searchAllRecordIds(String bitableBase) {
        ensureTenantToken();
        List<String> allRecordIds = new ArrayList<>();
        boolean hasMore = true;
        String pageToken = null;
        int pageSize = props.getPageSize();

        while (hasMore) {
            String url = FeishuBitableApiPaths.buildRecordsSearchUrl(bitableBase, pageSize, pageToken);

            HttpResponse response = HttpRequest.post(url)
                    .header("Authorization", currentFeishuAuth)
                    .header("Content-Type", "application/json; charset=utf-8")
                    .body("{}")
                    .execute();
            response = retryIfTokenExpired(response, url, "{}");

            String resp = response.body();
            ensureHttpOk(response, "records/search");
            JSONObject result = JSONUtil.parseObj(resp);
            JSONObject data = ensureFeishuSuccess(result, "records/search");

            JSONArray items = data.getJSONArray("items");
            if (items != null) {
                for (int j = 0; j < items.size(); j++) {
                    String recordId = items.getJSONObject(j).getStr("record_id");
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

    private void batchDeleteRecords(String bitableBase, List<String> recordIds) {
        ensureTenantToken();
        int batchSize = props.getBatchLimit();
        for (int i = 0; i < recordIds.size(); i += batchSize) {
            List<String> batch = recordIds.subList(i, Math.min(i + batchSize, recordIds.size()));

            JSONObject body = new JSONObject();
            body.set("records", batch);

            String path = bitableBase + FeishuBitableApiPaths.RECORDS_BATCH_DELETE;
            HttpResponse response = HttpRequest.post(path)
                    .header("Authorization", currentFeishuAuth)
                    .header("Content-Type", "application/json; charset=utf-8")
                    .body(body.toString())
                    .execute();
            response = retryIfTokenExpired(response, path, body.toString());

            String resp = response.body();
            ensureHttpOk(response, "records/batch_delete");
            ensureFeishuSuccess(JSONUtil.parseObj(resp), "records/batch_delete");
            System.out.println("删除第 " + (i / batchSize + 1) + " 批成功, 条数=" + batch.size());
        }
    }

    private void batchCreateRecords(
            String bitableBase,
            List<JSONObject> dataList,
            Set<String> existingFieldNames,
            List<ColumnBinding> bindings,
            Set<String> warnedMissingFields) {
        ensureTenantToken();
        int batchSize = props.getBatchLimit();
        for (int i = 0; i < dataList.size(); i += batchSize) {
            List<JSONObject> batch = dataList.subList(i, Math.min(i + batchSize, dataList.size()));

            JSONArray records = new JSONArray();
            for (JSONObject item : batch) {
                JSONObject fields = buildFieldsFromBindings(item, existingFieldNames, bindings, warnedMissingFields);
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

            String path = bitableBase + FeishuBitableApiPaths.RECORDS_BATCH_CREATE;
            HttpResponse response = HttpRequest.post(path)
                    .header("Authorization", currentFeishuAuth)
                    .header("Content-Type", "application/json; charset=utf-8")
                    .body(body.toString())
                    .execute();
            response = retryIfTokenExpired(response, path, body.toString());

            String resp = response.body();
            ensureHttpOk(response, "records/batch_create");
            ensureFeishuSuccess(JSONUtil.parseObj(resp), "records/batch_create");
            System.out.println("写入第 " + (i / batchSize + 1) + " 批成功, 条数=" + records.size());
        }
    }

    private void batchCreateRecordsLegacy(
            String bitableBase,
            List<JSONObject> dataList,
            Set<String> existingFieldNames,
            Set<String> warnedMissingFields) {
        ensureTenantToken();
        int batchSize = props.getBatchLimit();
        for (int i = 0; i < dataList.size(); i += batchSize) {
            List<JSONObject> batch = dataList.subList(i, Math.min(i + batchSize, dataList.size()));

            JSONArray records = new JSONArray();
            for (JSONObject item : batch) {
                JSONObject fields = buildFieldsLegacy(item, existingFieldNames, warnedMissingFields);
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

            String path = bitableBase + FeishuBitableApiPaths.RECORDS_BATCH_CREATE;
            HttpResponse response = HttpRequest.post(path)
                    .header("Authorization", currentFeishuAuth)
                    .header("Content-Type", "application/json; charset=utf-8")
                    .body(body.toString())
                    .execute();
            response = retryIfTokenExpired(response, path, body.toString());

            String resp = response.body();
            ensureHttpOk(response, "records/batch_create");
            ensureFeishuSuccess(JSONUtil.parseObj(resp), "records/batch_create");
            System.out.println("写入第 " + (i / batchSize + 1) + " 批成功, 条数=" + records.size());
        }
    }

    private static JSONObject buildFieldsFromBindings(
            JSONObject item,
            Set<String> existingFieldNames,
            List<ColumnBinding> bindings,
            Set<String> warnedMissingFields) {
        JSONObject fields = new JSONObject();
        for (ColumnBinding b : bindings) {
            Object val = extractValue(item, b.getSourceKey());
            if (val == null && b.getDefaultValue() != null) {
                val = b.getDefaultValue();
            }
            putIfFieldExists(fields, existingFieldNames, b.getFeishuField(), val, warnedMissingFields);
        }
        return fields;
    }

    private static Object extractValue(JSONObject item, String sourceKey) {
        if (sourceKey == null || sourceKey.isBlank()) {
            return null;
        }
        if (sourceKey.indexOf('.') >= 0) {
            return JSONUtil.getByPath(item, sourceKey);
        }
        return item.get(sourceKey);
    }

    private static JSONObject buildFieldsLegacy(JSONObject item, Set<String> existingFieldNames, Set<String> warnedMissingFields) {
        JSONObject fields = new JSONObject();
        putIfFieldExists(fields, existingFieldNames, "日期", item.getStr("date"), warnedMissingFields);
        putIfFieldExists(fields, existingFieldNames, "注册数", item.getInt("registerCount", 0), warnedMissingFields);
        putIfFieldExists(fields, existingFieldNames, "第1天", item.getStr("day1Rate", "-"), warnedMissingFields);
        putIfFieldExists(fields, existingFieldNames, "第2天", item.getStr("day2Rate", "-"), warnedMissingFields);
        putIfFieldExists(fields, existingFieldNames, "第3天", item.getStr("day3Rate", "-"), warnedMissingFields);
        putIfFieldExists(fields, existingFieldNames, "第4天", item.getStr("day4Rate", "-"), warnedMissingFields);
        putIfFieldExists(fields, existingFieldNames, "第5天", item.getStr("day5Rate", "-"), warnedMissingFields);
        putIfFieldExists(fields, existingFieldNames, "第6天", item.getStr("day6Rate", "-"), warnedMissingFields);
        putIfFieldExists(fields, existingFieldNames, "第7天", item.getStr("day7Rate", "-"), warnedMissingFields);
        putIfFieldExists(fields, existingFieldNames, "第8天", item.getStr("day8Rate", "-"), warnedMissingFields);
        return fields;
    }

    private static void putIfFieldExists(
            JSONObject fields,
            Set<String> existingFieldNames,
            String fieldName,
            Object value,
            Set<String> warnedMissingFields) {
        if (existingFieldNames.contains(fieldName)) {
            fields.set(fieldName, value);
            return;
        }
        if (warnedMissingFields.add(fieldName)) {
            System.out.println("飞书表缺少字段，已跳过写入: " + fieldName);
        }
    }

    private Set<String> fetchExistingFieldNames(String bitableBase) {
        ensureTenantToken();
        Set<String> fieldNames = new HashSet<>();
        String pageToken = null;
        boolean hasMore = true;
        int pageSize = props.getPageSize();
        while (hasMore) {
            String url = FeishuBitableApiPaths.buildFieldsListUrl(bitableBase, pageSize, pageToken);
            HttpResponse response = HttpRequest.get(url)
                    .header("Authorization", currentFeishuAuth)
                    .execute();
            response = retryIfTokenExpired(response, url, null);

            String bodyStr = response.body();
            ensureHttpOk(response, "fields/list");
            JSONObject result = JSONUtil.parseObj(bodyStr);
            JSONObject data = ensureFeishuSuccess(result, "fields/list");

            JSONArray items = data.getJSONArray("items");
            if (items != null) {
                for (int j = 0; j < items.size(); j++) {
                    String fieldName = items.getJSONObject(j).getStr("field_name");
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

    private void ensureHttpOk(HttpResponse response, String action) {
        if (response.getStatus() != 200) {
            throw new IllegalStateException(action + " http_status=" + response.getStatus() + ", body=" + response.body());
        }
    }

    private HttpResponse retryIfTokenExpired(HttpResponse response, String url, String body) {
        if (response.getStatus() != 401) {
            return response;
        }
        if (!isTokenExpiredResponse(response.body())) {
            return response;
        }

        refreshTenantAccessTokenInternal();
        System.out.println("检测到飞书 token 过期，已刷新 tenant_access_token 并重试");

        if (body == null) {
            return HttpRequest.get(url)
                    .header("Authorization", currentFeishuAuth)
                    .execute();
        }
        return HttpRequest.post(url)
                .header("Authorization", currentFeishuAuth)
                .header("Content-Type", "application/json; charset=utf-8")
                .body(body)
                .execute();
    }

    private static boolean isTokenExpiredResponse(String responseBody) {
        try {
            JSONObject json = JSONUtil.parseObj(responseBody);
            int code = json.getInt("code", -1);
            return code == 99991677;
        } catch (Exception ignore) {
            return false;
        }
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
