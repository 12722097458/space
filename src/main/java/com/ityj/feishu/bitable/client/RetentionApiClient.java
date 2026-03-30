package com.ityj.feishu.bitable.client;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.ityj.feishu.bitable.config.RetentionApiSettings;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 留存等业务接口的 HTTP 拉取（通过 {@link RemoteHttpClient}）。
 */
@Service
@RequiredArgsConstructor
public class RetentionApiClient {

    private static final String DEFAULT_START_PARAM = "startTime";
    private static final String DEFAULT_END_PARAM = "endTime";

    private final RemoteHttpClient remoteHttpClient;

    /**
     * GET 留存日报等非分页接口，解析 {@code { "code":0, "data": [ {...}, ... ] }}。
     */
    public List<JSONObject> fetchDailyRows(RetentionApiSettings settings, String startTime, String endTime) {
        if (settings == null) {
            throw new IllegalArgumentException("retention 配置不能为空");
        }
        if (settings.getUrl() == null || settings.getUrl().isBlank()) {
            throw new IllegalStateException("feishu.bitable.retention.url（或当前表的 retention.url）未配置");
        }
        String startParam = settings.getStartParamName() != null && !settings.getStartParamName().isBlank()
                ? settings.getStartParamName()
                : DEFAULT_START_PARAM;
        String endParam = settings.getEndParamName() != null && !settings.getEndParamName().isBlank()
                ? settings.getEndParamName()
                : DEFAULT_END_PARAM;

        URI uri = UriComponentsBuilder.fromUriString(settings.getUrl())
                .queryParam(startParam, startTime)
                .queryParam(endParam, endTime)
                .encode(StandardCharsets.UTF_8)
                .build()
                .toUri();

        Map<String, String> headers = new LinkedHashMap<>();
        if (settings.getAuthorization() != null && !settings.getAuthorization().isBlank()) {
            headers.put("Authorization", settings.getAuthorization());
        }

        String raw = remoteHttpClient.get(uri, headers);
        JSONObject result = JSONUtil.parseObj(raw);
        ensureBizCode(result, "retention/daily");

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

    private static void ensureBizCode(JSONObject response, String action) {
        int code = response.getInt("code", -1);
        if (code != 0) {
            throw new IllegalStateException(action + " biz_code=" + code + ", resp=" + response);
        }
    }
}
