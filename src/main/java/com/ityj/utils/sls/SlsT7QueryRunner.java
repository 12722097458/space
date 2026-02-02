package com.ityj.utils.sls;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.QueriedLog;
import com.aliyun.openservices.log.request.GetLogsRequest;
import com.aliyun.openservices.log.response.GetLogsResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

@Slf4j
public class SlsT7QueryRunner {

    // 配置参数（请务必更换为有效的 AK/SK 和 Endpoint）
    private static final String ENDPOINT = "cn-shanghai.log.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
    private static final String ACCESS_KEY_SECRET = "ACCESS_KEY_SECRET";
    private static final String PROJECT = "k8s-log-ca29bbf76013145ae80715e56ce1f199c";
    private static final String LOGSTORE = "event-log";
    public static void main(String[] args) {
        // 1. 设置起始和结束日期
        // 既然 11-13 统计的是 11-12，假如入参是11-13 12:23分， 那么统计的是11-12的数据
        LocalDate startDate = LocalDate.of(2026, 1, 29);
        LocalDate endDate = LocalDate.now(); // 截止到今天

        String fileName = "sls_t7_conversion_results.txt";

        // 使用 try-with-resources 自动关闭文件流
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write("统计日期,T7转化人数");
            writer.newLine();

            LocalDate current = startDate;
            while (!current.isAfter(endDate)) {
                String dateStr = current.toString();

                System.out.println("正在查询日期: " + dateStr + " ...");
                long count = getT7WorkflowCreateCount(dateStr);
                // 【核心修正】：输出日期显示为 current 的前一天，以符合业务定义
                LocalDate businessDate = current.minusDays(1);

                // 2. 格式化写入文件： 2025-11-13, 123
                String line = businessDate + "," + count;
                writer.write(line);
                writer.newLine();

                // 实时刷新，防止程序崩溃导致数据丢失
                writer.flush();

                System.out.println("结果: " + line);

                current = current.plusDays(1);
            }

            System.out.println("---------------------------------------");
            System.out.println("所有统计完成！结果已保存至: " + fileName);

        } catch (IOException e) {
            log.error("文件写入失败: ", e);
        }
    }

    /**
     * 你的核心查询逻辑保持不变
     */
    public static long getT7WorkflowCreateCount(String signupDateStr) {
        Client client = new Client(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET);
        long totalCount = 0;
        try {
            java.time.LocalDate date = java.time.LocalDate.parse(signupDateStr);

            // 逻辑核对：
            // 如果 signupDateStr = 2025-11-13
            // signupStart = 11-13 减 8 天 = 11-05
            // signupEnd = 11-13 减 7 天 = 11-06 (即 11-05 那一天的 24 小时)
            // 统计的是 11-05 注册的用户在随后 7 天内的转化
            int signupStart = (int) date.minusDays(8).atStartOfDay().toEpochSecond(java.time.ZoneOffset.ofHours(8));
            int signupEnd = (int) date.minusDays(7).atStartOfDay().toEpochSecond(java.time.ZoneOffset.ofHours(8));
            int observationStart = (int) date.minusDays(7).atStartOfDay().toEpochSecond(java.time.ZoneOffset.ofHours(8));
            int observationEnd = (int) date.atStartOfDay().toEpochSecond(java.time.ZoneOffset.ofHours(8));

            String query = "* | SELECT COUNT(DISTINCT user_id) AS t7_count " +
                    "FROM log " +
                    "WHERE user_id IN (" +
                    "  SELECT user_id FROM log " +
                    "  WHERE event = 'user_signup' AND user_id IS NOT NULL " +
                    "  AND __time__ >= " + signupStart + " AND __time__ < " + signupEnd +
                    ") " +
                    "AND event = 'workflow_create' " +
                    "AND __time__ >= " + observationStart + " AND __time__ < " + observationEnd;

            GetLogsRequest request = new GetLogsRequest(PROJECT, LOGSTORE, signupStart, observationEnd, "", query);
            GetLogsResponse logsResponse = client.GetLogs(request);
            List<QueriedLog> logs = logsResponse.getLogs();

            if (logs != null && !logs.isEmpty()) {
                for (LogContent mContent : logs.get(0).mLogItem.mContents) {
                    if ("t7_count".equals(mContent.mKey)) {
                        totalCount = Long.parseLong(mContent.mValue);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("查询日期 " + signupDateStr + " 出错: ", e);
        }
        return totalCount;
    }

}