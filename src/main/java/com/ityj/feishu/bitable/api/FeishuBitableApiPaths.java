package com.ityj.feishu.bitable.api;

/**
 * 飞书多维表格 Open API 路径（相对于
 * {@code .../open-apis/bitable/v1/apps/{app_token}/tables/{table_id}}）。
 * <p>
 * 官方文档：<a href="https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview">多维表格概述</a>
 */
public final class FeishuBitableApiPaths {

    private FeishuBitableApiPaths() {
    }

    /** POST 新增单条记录 */
    public static final String RECORDS = "/records";

    /** POST 查询记录 */
    public static final String RECORDS_SEARCH = "/records/search";

    /** POST 批量删除记录 */
    public static final String RECORDS_BATCH_DELETE = "/records/batch_delete";

    /** POST 批量新增记录 */
    public static final String RECORDS_BATCH_CREATE = "/records/batch_create";

    /** GET 列出字段 */
    public static final String FIELDS = "/fields";

    public static final String QUERY_PAGE_SIZE = "page_size";
    public static final String QUERY_PAGE_TOKEN = "page_token";

    /**
     * 构造「查询记录」分页请求 URL。
     */
    public static String buildRecordsSearchUrl(String bitableBase, int pageSize, String pageToken) {
        String url = bitableBase + RECORDS_SEARCH + "?" + QUERY_PAGE_SIZE + "=" + pageSize;
        if (pageToken != null) {
            url += "&" + QUERY_PAGE_TOKEN + "=" + pageToken;
        }
        return url;
    }

    /**
     * 构造「列出字段」分页请求 URL。
     */
    public static String buildFieldsListUrl(String bitableBase, int pageSize, String pageToken) {
        String url = bitableBase + FIELDS + "?" + QUERY_PAGE_SIZE + "=" + pageSize;
        if (pageToken != null) {
            url += "&" + QUERY_PAGE_TOKEN + "=" + pageToken;
        }
        return url;
    }
}
