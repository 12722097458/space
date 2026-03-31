package com.ityj.feishu.bitable.sync;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.ityj.feishu.bitable.api.FeishuBitableApiPaths;
import com.ityj.feishu.bitable.config.FeishuBitableProperties;
import com.ityj.feishu.bitable.config.FeishuTableProfile;
import com.ityj.feishu.bitable.mapping.ColumnBinding;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 飞书多维表格：支持多 {@code table_id} 与不同数据源、列映射（表头），以及预组装行数据写入。
 */
@Slf4j
@Service
public class FeishuBitableSyncService {
    private static final ZoneId ZONE_BEIJING = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final FeishuBitableProperties props;
    private final RestTemplate restTemplate;

    private static final String RETENTION_API_URL =
            "http://admin.web.pandaai.online/admin-api/user/users/retention/daily";
    private static final String PANDA_ADMIN_API_AUTH = "Bearer test1";

    private static final String STATS_CUSTOMERS_API_URL =
            "http://admin.web.pandaai.online/admin-api/user/sls-stats/stats-customers";

    /**
     * SLS 统计指标接口（单天），用于拼出飞书多维表“指标总览”一行数据。
     */
    private static final String STATS_METRICS_API_URL =
            "http://admin.web.pandaai.online/admin-api/user/sls-stats/stats";

    private static final String PAID_STATS_API_URL =
            "http://admin.web.pandaai.online/admin-api/course/order/stats-by-paid-at";

    private String currentFeishuAuth;
    private long tenantTokenExpiresAtEpochMs;

    public FeishuBitableSyncService(FeishuBitableProperties props, RestTemplate restTemplate) {
        this.props = props;
        this.restTemplate = restTemplate;
    }

    /**
     * @param profileKey {@code feishu.bitable.tables} 中的 key（如 {@code retention_daily}）
     */
    public void sync(String profileKey, String startTime, String endTime) {
        FeishuTableProfile profile = resolveProfile(profileKey);
        String bitableBase = props.bitableBaseUrlForTable(profile.getTableId());

        List<JSONObject> retentionData = fetchRetentionData(profileKey, startTime, endTime);
        List<ColumnBinding> bindings = profile.getColumns();

        sync(profileKey, bitableBase, retentionData, bindings);
    }

    /**
     * 同步指定多维表格的数据（调用方直接提供数据和列映射）。
     *
     * @param profileKey    逻辑表名，仅用于日志
     * @param bitableBase   当前表的根路径，例如 {@code https://.../apps/{appToken}/tables/{tableId}}
     * @param retentionData 待写入的业务数据列表
     * @param bindings      列映射配置（飞书列名 <- 源 JSON 字段路径）
     */
    public void sync(String profileKey,
                     String bitableBase,
                     List<JSONObject> retentionData,
                     List<ColumnBinding> bindings) {
        Set<String> warned = new HashSet<>();

        if (retentionData == null || retentionData.isEmpty()) {
            log.info("[{}] 数据为空，跳过写入", profileKey);
            return;
        }
        if (bindings == null || bindings.isEmpty()) {
            log.warn("[{}] columns 为空，无法进行字段映射，已跳过写入", profileKey);
            return;
        }

        log.info("[{}] 查到业务数据 {} 条", profileKey, retentionData.size());

        List<String> recordIds = searchAllRecordIds(bitableBase);
        log.info("[{}] 查到历史记录 {} 条", profileKey, recordIds.size());

        if (!recordIds.isEmpty()) {
            batchDeleteRecords(bitableBase, recordIds);
            log.info("[{}] 已清空表格历史数据", profileKey);
        }

        Set<String> existingFieldNames = fetchExistingFieldNames(bitableBase);
        log.info("[{}] 飞书表字段数: {}", profileKey, existingFieldNames.size());
        batchCreateRecords(bitableBase, retentionData, existingFieldNames, bindings, warned);
        log.info("[{}] 数据写入完成", profileKey);
    }

    /**
     * 获取飞书多维表格的业务数据。
     * <p>
     * 注意：该方法用于“非 HTTP 数据源”的接入（例如直接从内部计算/DB/Redis/本地文件等）。
     * 当前先提供占位实现，后续你可以在这里按业务接入真实数据源。
     *
     * @return 期望写入表格的一行数据列表（每个元素是源 JSON 对象，供列映射提取字段）
     */
    private List<JSONObject> fetchRetentionData(String profileKey, String startTime, String endTime) {
        String url = RETENTION_API_URL + "?startTime=" + startTime + "&endTime=" + endTime;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", PANDA_ADMIN_API_AUTH);
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        try {
            log.info("[{}] 调用留存数据接口, url={}", profileKey, url);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                log.error("[{}] 调用留存数据接口失败, httpStatus={}, body={}", profileKey,
                        response.getStatusCode().value(), response.getBody());
                return Collections.emptyList();
            }

            String body = response.getBody();
            if (body == null || body.isBlank()) {
                log.warn("[{}] 留存数据接口返回空响应", profileKey);
                return Collections.emptyList();
            }

            JSONObject json = JSONUtil.parseObj(body);
            int code = json.getInt("code", -1);
            if (code != 0) {
                log.error("[{}] 留存数据接口业务失败, code={}, msg={}", profileKey, code, json.getStr("msg"));
                return Collections.emptyList();
            }

            JSONArray dataArr = json.getJSONArray("data");
            if (dataArr == null || dataArr.isEmpty()) {
                log.info("[{}] 留存数据接口返回 data 为空", profileKey);
                return Collections.emptyList();
            }

            List<JSONObject> list = new ArrayList<>(dataArr.size());
            for (int i = 0; i < dataArr.size(); i++) {
                JSONObject item = dataArr.getJSONObject(i);
                if (item != null) {
                    list.add(item);
                }
            }
            log.info("[{}] 留存数据接口返回记录数: {}", profileKey, list.size());
            return list;
        } catch (RestClientException ex) {
            log.error("[{}] 调用留存数据接口出现异常", profileKey, ex);
            return Collections.emptyList();
        } catch (Exception ex) {
            log.error("[{}] 解析留存数据接口响应出现异常", profileKey, ex);
            return Collections.emptyList();
        }
    }

    /**
     * 获取收入统计数据（订单按支付时间聚合）。
     *
     * @param profileKey 日志标识
     * @param date       统计日期（yyyy-MM-dd）
     * @return data 对象（JSON），若无数据则返回 null
     */
    private JSONObject fetchRevenueStats(String profileKey, String startDate, String endDate) {
        String url = PAID_STATS_API_URL + "?startTime=" + startDate + "&endTime=" + endDate;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", PANDA_ADMIN_API_AUTH);
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        try {
            log.info("[{}] 调用收入统计接口, url={}", profileKey, url);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                log.error("[{}] 调用收入统计接口失败, httpStatus={}, body={}", profileKey,
                        response.getStatusCode().value(), response.getBody());
                return null;
            }

            String body = response.getBody();
            if (body == null || body.isBlank()) {
                log.warn("[{}] 收入统计接口返回空响应", profileKey);
                return null;
            }

            JSONObject json = JSONUtil.parseObj(body);
            int code = json.getInt("code", -1);
            if (code != 0) {
                log.error("[{}] 收入统计接口业务失败, code={}, msg={}", profileKey, code, json.getStr("msg"));
                return null;
            }

            JSONObject data = json.getJSONObject("data");
            if (data == null) {
                log.info("[{}] 收入统计接口返回 data 为空", profileKey);
                return null;
            }
            return data;
        } catch (RestClientException ex) {
            log.error("[{}] 调用收入统计接口出现异常", profileKey, ex);
            return null;
        } catch (Exception ex) {
            log.error("[{}] 解析收入统计接口响应出现异常", profileKey, ex);
            return null;
        }
    }

    /**
     * 覆盖写入“新老客占比”表格。
     *
     * @param date 统计日期（yyyy-MM-dd）
     * @return 实际写入的行数
     */
    public int syncCustomerRatio(String date) {
        String profileKey = "customer_ratio_daily";
        FeishuTableProfile profile = resolveProfile(profileKey);
        String bitableBase = props.bitableBaseUrlForTable(profile.getTableId());

        JSONObject metrics = fetchCustomerStats(profileKey, date);
        if (metrics == null) {
            log.warn("[{}] metrics 为空，跳过写入", profileKey);
            return 0;
        }

        List<JSONObject> rows = new ArrayList<>(2);

        JSONObject registerRow = new JSONObject();
        registerRow.set("label", "注册");
        registerRow.set("count", metrics.getInt("registrations", 0));
        rows.add(registerRow);

        JSONObject returningRow = new JSONObject();
        returningRow.set("label", "老客");
        returningRow.set("count", metrics.getInt("returningCustomers", 0));
        rows.add(returningRow);

        List<ColumnBinding> bindings = profile.getColumns();
        sync(profileKey, bitableBase, rows, bindings);
        return rows.size();
    }

    /**
     * 覆盖写入“新客漏斗”表格。
     *
     * @param date 统计日期（yyyy-MM-dd）
     * @return 实际写入的行数
     */
    public int syncNewCustomerFunnel(String date) {
        String profileKey = "new_customer_funnel_daily";
        FeishuTableProfile profile = resolveProfile(profileKey);
        String bitableBase = props.bitableBaseUrlForTable(profile.getTableId());

        JSONObject metrics = fetchCustomerStats(profileKey, date);
        if (metrics == null) {
            log.warn("[{}] metrics 为空，跳过写入", profileKey);
            return 0;
        }

        List<JSONObject> rows = new ArrayList<>(4);

        JSONObject visitRow = new JSONObject();
        visitRow.set("label", "新客访问");
        visitRow.set("value", metrics.getInt("newVisitorVisits", 0));
        rows.add(visitRow);

        JSONObject verifyRow = new JSONObject();
        verifyRow.set("label", "验证");
        verifyRow.set("value", metrics.getInt("verifications", 0));
        rows.add(verifyRow);

        JSONObject registerRow = new JSONObject();
        registerRow.set("label", "注册");
        registerRow.set("value", metrics.getInt("registrations", 0));
        rows.add(registerRow);

        JSONObject t0Row = new JSONObject();
        t0Row.set("label", "T0创建");
        t0Row.set("value", metrics.getInt("t0WorkflowCreations", 0));
        rows.add(t0Row);

        List<ColumnBinding> bindings = profile.getColumns();
        sync(profileKey, bitableBase, rows, bindings);
        return rows.size();
    }

    /**
     * 获取用户统计数据（新客访问、验证、注册、老客、T0 创建等指标）。
     *
     * @param profileKey 日志标识
     * @param date       统计日期（yyyy-MM-dd）
     * @return metrics 对象（JSON），若无数据则返回 null
     */
    private JSONObject fetchCustomerStats(String profileKey, String date) {
        String url = STATS_CUSTOMERS_API_URL + "?startTime=" + date + "&endTime=" + date;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", PANDA_ADMIN_API_AUTH);
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        try {
            log.info("[{}] 调用客户统计接口, url={}", profileKey, url);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                log.error("[{}] 调用客户统计接口失败, httpStatus={}, body={}", profileKey,
                        response.getStatusCode().value(), response.getBody());
                return null;
            }

            String body = response.getBody();
            if (body == null || body.isBlank()) {
                log.warn("[{}] 客户统计接口返回空响应", profileKey);
                return null;
            }

            JSONObject json = JSONUtil.parseObj(body);
            int code = json.getInt("code", -1);
            if (code != 0) {
                log.error("[{}] 客户统计接口业务失败, code={}, msg={}", profileKey, code, json.getStr("msg"));
                return null;
            }

            JSONArray dataArr = json.getJSONArray("data");
            if (dataArr == null || dataArr.isEmpty()) {
                log.info("[{}] 客户统计接口返回 data 为空", profileKey);
                return null;
            }

            JSONObject first = dataArr.getJSONObject(0);
            if (first == null) {
                log.warn("[{}] 客户统计接口 data[0] 为空", profileKey);
                return null;
            }
            JSONObject metrics = first.getJSONObject("metrics");
            if (metrics == null) {
                log.warn("[{}] 客户统计接口 metrics 为空", profileKey);
            }
            return metrics;
        } catch (RestClientException ex) {
            log.error("[{}] 调用客户统计接口出现异常", profileKey, ex);
            return null;
        } catch (Exception ex) {
            log.error("[{}] 解析客户统计接口响应出现异常", profileKey, ex);
            return null;
        }
    }

    /**
     * 调用 SLS 指标统计接口，获取指定日期的完整数据行（包含 date 与 metrics）。
     *
     * @param profileKey 日志标识
     * @param date       统计日期（yyyy-MM-dd）
     * @return metrics 对象（JSON），若无数据则返回 null
     */
    private JSONObject fetchSlsStatsRow(String profileKey, String date) {
        String url = STATS_METRICS_API_URL + "?startTime=" + date + "&endTime=" + date;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", PANDA_ADMIN_API_AUTH);
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        try {
            log.info("[{}] 调用 SLS 指标统计接口, url={}", profileKey, url);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                log.error("[{}] 调用 SLS 指标统计接口失败, httpStatus={}, body={}", profileKey,
                        response.getStatusCode().value(), response.getBody());
                return null;
            }

            String body = response.getBody();
            if (body == null || body.isBlank()) {
                log.warn("[{}] SLS 指标统计接口返回空响应", profileKey);
                return null;
            }

            JSONObject json = JSONUtil.parseObj(body);
            int code = json.getInt("code", -1);
            if (code != 0) {
                log.error("[{}] SLS 指标统计接口业务失败, code={}, msg={}", profileKey, code, json.getStr("msg"));
                return null;
            }

            JSONArray dataArr = json.getJSONArray("data");
            if (dataArr == null || dataArr.isEmpty()) {
                log.info("[{}] SLS 指标统计接口返回 data 为空", profileKey);
                return null;
            }

            JSONObject first = dataArr.getJSONObject(0);
            if (first == null) {
                log.warn("[{}] SLS 指标统计接口 data[0] 为空", profileKey);
                return null;
            }
            return first;
        } catch (RestClientException ex) {
            log.error("[{}] 调用 SLS 指标统计接口出现异常", profileKey, ex);
            return null;
        } catch (Exception ex) {
            log.error("[{}] 解析 SLS 指标统计接口响应出现异常", profileKey, ex);
            return null;
        }
    }

    /**
     * 覆盖写入“收入统计”表格。
     *
     * @param startDate 统计开始日期（yyyy-MM-dd）
     * @param endDate   统计结束日期（yyyy-MM-dd）
     * @return 实际写入的行数
     */
    public int syncRevenueStats(String startDate, String endDate) {
        String profileKey = "revenue_daily";
        FeishuTableProfile profile = resolveProfile(profileKey);
        String bitableBase = props.bitableBaseUrlForTable(profile.getTableId());

        LocalDate parsedStart = LocalDate.parse(startDate);
        LocalDate parsedEnd = LocalDate.parse(endDate);
        if (parsedStart.isAfter(parsedEnd)) {
            throw new IllegalArgumentException("startDate 不能晚于 endDate");
        }

        JSONObject data = fetchRevenueStats(profileKey, parsedStart.toString(), parsedEnd.toString());
        if (data == null) {
            log.warn("[{}] 收入统计 data 为空，跳过写入", profileKey);
            return 0;
        }

        JSONObject row = new JSONObject();
        row.set("time", data.getStr("endTime", parsedEnd.toString()));
        row.set("proMemberUserCount", data.getInt("proMemberUserCount", 0));
        row.set("computeRechargeUserCount", data.getInt("computeRechargeUserCount", 0));
        row.set("totalAmount", data.get("totalAmount"));

        List<JSONObject> rows = Collections.singletonList(row);
        List<ColumnBinding> bindings = profile.getColumns();
        sync(profileKey, bitableBase, rows, bindings);
        return rows.size();
    }

    /**
     * 追加写入“用户 SLS 指标总览”表格：每次调用在表末尾新增一行，不清空历史数据。
     *
     * @param dateStr    统计日期（yyyy-MM-dd）
     * @param profileKey 多维表 profileKey，例如 {@code sls_stats_daily}
     * @return 实际写入的行数（成功则为 1，失败/无数据为 0）
     */
    public int syncSlsStatsDaily(String dateStr, String profileKey) {
        LocalDate.parse(dateStr);

        FeishuTableProfile profile = resolveProfile(profileKey);

        JSONObject row = fetchSlsStatsRow(profileKey, dateStr);
        if (row == null) {
            log.warn("[{}] SLS 指标 data 为空，跳过写入", profileKey);
            return 0;
        }

        Map<String, Object> fields = buildFieldsForAppend(row, profile.getColumns());
        if (fields.isEmpty()) {
            log.warn("[{}] 构造出的字段 Map 为空，跳过写入", profileKey);
            return 0;
        }

        appendRecord(profileKey, fields);
        return 1;
    }

    private static Map<String, Object> buildFieldsForAppend(JSONObject item, List<ColumnBinding> bindings) {
        Map<String, Object> fields = new LinkedHashMap<>();
        if (bindings == null || bindings.isEmpty()) {
            return fields;
        }
        for (ColumnBinding b : bindings) {
            Object val = extractValue(item, b.getSourceKey());
            if (val == null && b.getDefaultValue() != null) {
                val = b.getDefaultValue();
            }
            if (val != null) {
                fields.put(b.getFeishuField(), val);
            }
        }
        return fields;
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
            log.info("删除第 {} 批成功, 条数={}", (i / batchSize + 1), batch.size());
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
                log.info("第 {} 批无可写入字段，跳过", (i / batchSize + 1));
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
            log.info("写入第 {} 批成功, 条数={}", (i / batchSize + 1), records.size());
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

    private static void putIfFieldExists(
            JSONObject fields,
            Set<String> existingFieldNames,
            String fieldName,
            Object value,
            Set<String> warnedMissingFields) {
        if (existingFieldNames.contains(fieldName)) {
            fields.set(fieldName, normalizeValueForFeishuField(fieldName, value));
            return;
        }
        if (warnedMissingFields.add(fieldName)) {
            log.info("飞书表缺少字段，已跳过写入: {}", fieldName);
        }
    }

    /**
     * 飞书 Date/DateTime 列要求 unix timestamp（毫秒），
     * 这里对常见“日期/时间”字段名做统一转换，兼容 yyyy-MM-dd / yyyy/MM/dd / yyyy-MM-dd HH:mm:ss。
     */
    private static Object normalizeValueForFeishuField(String fieldName, Object value) {
        if (!(value instanceof String raw)) {
            return value;
        }
        if (fieldName == null || (!fieldName.contains("日期") && !fieldName.contains("时间"))) {
            return value;
        }
        String text = raw.trim();
        if (text.isEmpty()) {
            return value;
        }

        try {
            LocalDate date = LocalDate.parse(text.replace('/', '-'));
            return date.atStartOfDay(ZONE_BEIJING).toInstant().toEpochMilli();
        } catch (Exception ignore) {
            // ignore
        }

        try {
            LocalDateTime dateTime = LocalDateTime.parse(text.replace('/', '-'), DATE_TIME_FORMATTER);
            return dateTime.atZone(ZONE_BEIJING).toInstant().toEpochMilli();
        } catch (Exception ignore) {
            return value;
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
        log.info("检测到飞书 token 过期，已刷新 tenant_access_token 并重试");

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
