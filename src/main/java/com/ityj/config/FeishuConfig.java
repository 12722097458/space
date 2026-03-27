package com.ityj.config;

import org.springframework.stereotype.Component;

@Component
public class FeishuConfig {

    public static final String APP_ID = "cli_a94fb5909138dbde";
    public static final String APP_SECRET = "F5pbn33ngrK1YX4zpC80ibezKCKp1gf7";

    public static final String APP_TOKEN = "AoX0w6TfoiwCHCkWdjsczXaFnzd";
    public static final String TABLE_ID = "tbl1u20RQ4Q0lUz6";

    public static final String BASE_URL =
            "https://open.feishu.cn/open-apis/bitable/v1/apps/"
                    + APP_TOKEN + "/tables/" + TABLE_ID;
}