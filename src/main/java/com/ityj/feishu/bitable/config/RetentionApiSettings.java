package com.ityj.feishu.bitable.config;

import lombok.Getter;
import lombok.Setter;

/**
 * 留存等业务 HTTP 接口配置，绑定 {@code feishu.bitable.retention} 或
 * {@code feishu.bitable.tables.<key>.retention}。
 */
@Getter
@Setter
public class RetentionApiSettings {

    /** 完整 URL（不含查询参数） */
    private String url;

    /** 请求头 Authorization，一般为 Bearer 开头 */
    private String authorization;

    /** 查询参数名：开始时间 */
    private String startParamName;

    /** 查询参数名：结束时间 */
    private String endParamName;
}
