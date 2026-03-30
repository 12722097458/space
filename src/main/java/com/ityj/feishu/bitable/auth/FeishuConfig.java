package com.ityj.feishu.bitable.auth;

import org.springframework.stereotype.Component;

/**
 * 飞书应用密钥： app-id 配合获取 tenant_access_token。
 */
@Component
public class FeishuConfig {

    private static final String DEFAULT_APP_SECRET = "F5pbn33ngrK1YX4zpC80ibezKCKp1gf7";

    /** 优先使用环境变量 {@code FEISHU_APP_SECRET}，未设置时回退到默认值（本地开发）。 */
    public static String appSecret() {
        String env = System.getenv("FEISHU_APP_SECRET");
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        return DEFAULT_APP_SECRET;
    }
}
