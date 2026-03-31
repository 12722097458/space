package com.ityj.feishu.bitable.config;

import com.ityj.feishu.bitable.sync.FeishuBitableSyncService;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 飞书多维表格同步相关配置（URL、鉴权、批大小、分页等）。
 * <p>
 * 连接参数、批大小、分页等请在 {@code application.yml} 的 {@code feishu.bitable} 下配置，
 * 可用环境变量覆盖，例如 {@code FEISHU_APP_ID}、{@code FEISHU_APP_SECRET}、{@code FEISHU_APP_TOKEN}。
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "feishu.bitable")
public class FeishuBitableProperties {

    private String appId;

    /** 用于获取 {@code tenant_access_token} */
    private String appSecret;

    private String appToken;

    /**
     * 多维表格 OpenAPI 根路径（不含 app_token / table_id），例如：
     * {@code https://open.feishu.cn/open-apis/bitable/v1/apps}
     */
    private String openApiBaseUrl;

    private String tenantAccessTokenUrl;

    private int batchLimit;
    private int pageSize;

    /**
     * 多表格配置：key 为逻辑名（如 {@code retention_daily}、{@code channel-a}），用于
     * {@link FeishuBitableSyncService#sync(String, String, String)}。
     */
    @Setter(AccessLevel.NONE)
    private Map<String, FeishuTableProfile> tables = new LinkedHashMap<>();

    public void setTables(Map<String, FeishuTableProfile> tables) {
        this.tables = tables != null ? tables : new LinkedHashMap<>();
    }

    /**
     * 指定 {@code tableId}（同一 app_token）时的多维表格 API 根路径。
     */
    public String bitableBaseUrlForTable(String tableId) {
        return bitableBaseUrlForTable(appToken, tableId);
    }

    public String bitableBaseUrlForTable(String appTokenForTable, String tableIdValue) {
        String base = openApiBaseUrl.endsWith("/")
                ? openApiBaseUrl.substring(0, openApiBaseUrl.length() - 1)
                : openApiBaseUrl;
        return base + "/" + appTokenForTable + "/tables/" + tableIdValue;
    }
}
