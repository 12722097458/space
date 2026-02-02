package com.ityj.utils.sls;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.QueriedLog;
import com.aliyun.openservices.log.request.GetLogsRequest;
import com.aliyun.openservices.log.response.GetLogsResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

@Slf4j
public class SlsRetentionQueryRunner {

    private static final String ENDPOINT = "cn-shanghai.log.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
    private static final String ACCESS_KEY_SECRET = "ACCESS_KEY_SECRET";
    private static final String PROJECT = "k8s-log-ca29bbf76013145ae80715e56ce1f199c";
    private static final String LOGSTORE = "event-log";

    public static void main(String[] args) {
        // 从 2025-11-13 开始跑数据
        LocalDate startDate = LocalDate.of(2025, 11, 13);
        LocalDate endDate = LocalDate.now();

        String fileName = "工作流及留存率统计图表retention_results.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write("日期,T7留存率(%),T15留存率(%),T30留存率(%)");
            writer.newLine();

            LocalDate current = startDate;
            while (!current.isAfter(endDate)) {
                System.out.println("正在计算日期: " + current + " 的留存数据...");
                String resultLine = getRetentionData(current);

                // 【核心修正】：输出日期显示为 current 的前一天，以符合业务定义
                LocalDate businessDate = current.minusDays(1);
                
                writer.write(businessDate + "," + resultLine);
                writer.newLine();
                writer.flush();
                
                current = current.plusDays(1);
            }
            System.out.println("统计任务完成，结果已保存至: " + fileName);
        } catch (Exception e) {
            log.error("执行批量任务失败", e);
        }
    }

    public static String getRetentionData(LocalDate baseDate) {
        Client client = new Client(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET);
        
        // 计算 SQL 中各个时间节点的 Unix 时间戳
        // T7 相关 (对应原SQL -8 to -7)
        long t7SignupStart = baseDate.minusDays(8).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long t7SignupEnd = baseDate.minusDays(7).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        
        // T15 相关 (对应原SQL -16 to -15)
        long t15SignupStart = baseDate.minusDays(16).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long t15SignupEnd = baseDate.minusDays(15).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        
        // T30 相关 (对应原SQL -31 to -30)
        long t30SignupStart = baseDate.minusDays(31).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long t30SignupEnd = baseDate.minusDays(30).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

        // 统一的观察结束时间 (对应原SQL的 current_date，即 baseDate 当天 0 点)
        long nowTime = baseDate.atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

        // 构建复杂的聚合 SQL
        String query = String.format(
            "* | SELECT " +
            "  CASE WHEN t13.cnt=0 THEN 0 ELSE ROUND(t12.cnt * 100.0 / t13.cnt, 2) END as t7_rate, " +
            "  CASE WHEN t15.cnt=0 THEN 0 ELSE ROUND(t14.cnt * 100.0 / t15.cnt, 2) END as t15_rate, " +
            "  CASE WHEN t17.cnt=0 THEN 0 ELSE ROUND(t16.cnt * 100.0 / t17.cnt, 2) END as t30_rate " +
            "FROM " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) AND __time__>=%d AND __time__<%d) t12, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) t13, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) AND __time__>=%d AND __time__<%d) t14, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) t15, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) AND __time__>=%d AND __time__<%d) t16, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) t17",
            t7SignupStart, t7SignupEnd, t7SignupEnd, nowTime,
            t7SignupStart, t7SignupEnd,
            t15SignupStart, t15SignupEnd, t15SignupEnd, nowTime,
            t15SignupStart, t15SignupEnd,
            t30SignupStart, t30SignupEnd, t30SignupEnd, nowTime,
            t30SignupStart, t30SignupEnd
        );

        try {
            // GetLogs 范围必须覆盖最远的历史日期 (T-31) 到现在
            GetLogsRequest request = new GetLogsRequest(PROJECT, LOGSTORE, (int)t30SignupStart, (int)nowTime, "", query);
            GetLogsResponse response = client.GetLogs(request);
            List<QueriedLog> logs = response.getLogs();

            if (logs != null && !logs.isEmpty()) {
                String r7 = "0", r15 = "0", r30 = "0";
                for (LogContent c : logs.get(0).mLogItem.mContents) {
                    if ("t7_rate".equals(c.mKey)) r7 = c.mValue;
                    if ("t15_rate".equals(c.mKey)) r15 = c.mValue;
                    if ("t30_rate".equals(c.mKey)) r30 = c.mValue;
                }
                return r7 + "," + r15 + "," + r30;
            }
        } catch (Exception e) {
            log.error("Query Error for date " + baseDate + ": " + e.getMessage());
        }
        return "0,0,0";
    }
}