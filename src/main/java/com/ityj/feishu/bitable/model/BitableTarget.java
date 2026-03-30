package com.ityj.feishu.bitable.model;

import lombok.Getter;

import java.util.Objects;

/**
 * 目标多维表格（同一 app 下不同 {@code table_id}，可选覆盖 app_token）。
 */
@Getter
public final class BitableTarget {

    private final String tableId;
    /** 为 null 时使用全局配置中的 app_token */
    private final String appTokenOverride;

    public BitableTarget(String tableId) {
        this(tableId, null);
    }

    public BitableTarget(String tableId, String appTokenOverride) {
        this.tableId = Objects.requireNonNull(tableId, "tableId").trim();
        this.appTokenOverride = appTokenOverride == null || appTokenOverride.isBlank() ? null : appTokenOverride.trim();
    }

}
