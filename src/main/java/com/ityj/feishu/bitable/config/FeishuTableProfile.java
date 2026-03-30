package com.ityj.feishu.bitable.config;

import com.ityj.feishu.bitable.mapping.ColumnBinding;
import com.ityj.feishu.bitable.sync.FeishuBitableSyncService;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * 单个表格：{@code table_id}、对应数据源（留存接口等）及列映射。
 */
@Getter
@Setter
public class FeishuTableProfile {

    private String tableId;

    /**
     * 列映射：飞书列名 &lt;- 源 JSON 字段路径。
     * 为空时若走 {@link FeishuBitableSyncService#sync(String, String, String)} 则使用内置留存字段映射。
     */
    private List<ColumnBinding> columns = new ArrayList<>();
}
