package com.ityj.feishu.bitable.mapping;

import lombok.Getter;
import lombok.Setter;

/**
 * 一行数据源字段到飞书列名的映射（表头）。
 * {@code sourceKey} 支持 Hutool 路径，如 {@code date}、{@code a.b}。
 */
@Getter
@Setter
public class ColumnBinding {

    private String feishuField;
    private String sourceKey;
    /** 当源 JSON 取不到值时写入的默认（多为文本列），可为 null */
    private String defaultValue;
}
