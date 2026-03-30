package com.ityj.feishu.bitable.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 在多维表格中新增一条记录（对应 OpenAPI：新增记录）。
 */
@Getter
@Setter
public class CreateBitableRecordRequest {

    /** 与 {@code feishu.bitable.tables} 的 key 一致，默认 {@code default} */
    private String profileKey = "default";

    /**
     * 列名 → 单元格值，格式需符合飞书字段类型（如日期为毫秒时间戳、人员为数组等）。
     */
    private Map<String, Object> fields = new LinkedHashMap<>();
}
