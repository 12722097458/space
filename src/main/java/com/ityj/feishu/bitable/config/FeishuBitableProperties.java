package com.ityj.feishu.bitable.config;

import com.ityj.feishu.bitable.sync.FeishuBitableSyncService;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 飞书多维表格同步相关配置（URL、鉴权、留存接口参数名等）。
 * <p>
 * 支持通过 {@code application.yml} 与环境变量覆盖，例如：
 * {@code FEISHU_APP_ID}、{@code FEISHU_APP_TOKEN}、{@code FEISHU_TABLE_ID}、{@code RETENTION_API_URL}。
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "feishu.bitable")
public class FeishuBitableProperties {

    private String appId = "cli_a94fb5909138dbde";
    private String appToken = "AoX0w6TfoiwCHCkWdjsczXaFnzd";
    private String tableId = "tbl1u20RQ4Q0lUz6";

    /**
     * 多维表格 OpenAPI 根路径（不含 app_token / table_id），例如：
     * {@code https://open.feishu.cn/open-apis/bitable/v1/apps}
     */
    private String openApiBaseUrl = "https://open.feishu.cn/open-apis/bitable/v1/apps";

    private String tenantAccessTokenUrl = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal";

    private int batchLimit = 500;
    private int pageSize = 100;

    private Retention retention = new Retention();

    /**
     * 多表格配置：key 为逻辑名（如 {@code default}、{@code channel-a}），用于
     * {@link FeishuBitableSyncService#sync(String, String, String)}。
     * 未配置时仍使用根级别的 {@link #tableId} 与 {@link #retention}。
     */
    @Setter(AccessLevel.NONE)
    private Map<String, FeishuTableProfile> tables = new LinkedHashMap<>();

    public void setTables(Map<String, FeishuTableProfile> tables) {
        this.tables = tables != null ? tables : new LinkedHashMap<>();
    }

    /**
     * 多维表格 API 根路径：{@code .../apps/{appToken}/tables/{tableId}}
     */
    public String bitableBaseUrl() {
        return bitableBaseUrlForTable(appToken, tableId);
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

    @Getter
    @Setter
    public static class Retention {

        /** 留存等业务接口完整 URL（不含查询参数） */
        private String url = "https://admin.web.pandaai.online/admin-api/user/users/retention/daily";

        /** 请求头 Authorization，一般为 Bearer 开头 */
        private String authorization = "Bearer test1";

        /** 查询参数名：开始时间（与后端约定，默认 startTime） */
        private String startParamName = "startTime";

        /** 查询参数名：结束时间（与后端约定，默认 endTime） */
        private String endParamName = "endTime";
    }
}
