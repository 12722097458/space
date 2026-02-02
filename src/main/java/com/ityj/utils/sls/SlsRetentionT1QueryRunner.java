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
import java.time.ZoneOffset;
import java.util.List;

@Slf4j
public class SlsRetentionT1QueryRunner {

    private static final String ENDPOINT = "cn-shanghai.log.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
    private static final String ACCESS_KEY_SECRET = "ACCESS_KEY_SECRET";
    private static final String PROJECT = "k8s-log-ca29bbf76013145ae80715e56ce1f199c";
    private static final String LOGSTORE = "event-log";

    public static void main(String[] args) {
        // 设置起始日期（2025-11-13）
        LocalDate startDate = LocalDate.of(2025, 11, 13);
        LocalDate endDate = LocalDate.now(); 

        String fileName = "留存率7-15-30图表sls_full_retention_data.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            // 写入表头（增加 T1 维度）
            writer.write("业务日期,T1活跃,T1注册,T7活跃,T7注册,T15活跃,T15注册,T30活跃,T30注册");
            writer.newLine();

            LocalDate current = startDate;
            while (!current.isAfter(endDate)) {
                // 业务逻辑：输出日期为入参的前一天
                LocalDate businessDate = current.minusDays(1);
                System.out.println("正在处理入参: " + current + " (业务日期: " + businessDate + ")");
                
                String rawData = getFullRetentionData(current);
                
                String line = businessDate + "," + rawData;
                writer.write(line);
                writer.newLine();
                writer.flush();

                System.out.println("写入结果: " + line);
                current = current.plusDays(1);
            }
            System.out.println("---------------------------------------");
            System.out.println("任务成功完成！结果文件: " + fileName);
        } catch (IOException e) {
            log.error("文件写入失败", e);
        }
    }

    public static String getFullRetentionData(LocalDate date) {
        Client client = new Client(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET);
        
        // --- 1. 计算时间戳 ---
        // T1 (对应SQL -2 to -1)
        long t1S = date.minusDays(2).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long t1E = date.minusDays(1).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        
        // T7 (对应SQL -8 to -7)
        long t7S = date.minusDays(8).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long t7E = date.minusDays(7).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        
        // T15 (对应SQL -16 to -15)
        long t15S = date.minusDays(16).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long t15E = date.minusDays(15).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        
        // T30 (对应SQL -31 to -30)
        long t30S = date.minusDays(31).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long t30E = date.minusDays(30).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        
        // 截止时间（T日0点）
        long now = date.atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

        // --- 2. 构造包含 T1 到 T30 的全量 SQL ---
        String query = String.format(
            "* | SELECT " +
            " t1.cnt as t1a, t2.cnt as t1s, t3.cnt as t7a, t4.cnt as t7s, " +
            " t5.cnt as t15a, t6.cnt as t15s, t7.cnt as t30a, t8.cnt as t30s " +
            "FROM " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) AND __time__>=%d AND __time__<%d) t1, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) t2, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) AND __time__>=%d AND __time__<%d) t3, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) t4, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) AND __time__>=%d AND __time__<%d) t5, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) t6, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) AND __time__>=%d AND __time__<%d) t7, " +
            " (SELECT COUNT(DISTINCT user_id) as cnt FROM log WHERE event='user_signup' AND __time__>=%d AND __time__<%d) t8",
            t1S, t1E, t1E, now, t1S, t1E,  // T1
            t7S, t7E, t7E, now, t7S, t7E,  // T7
            t15S, t15E, t15E, now, t15S, t15E, // T15
            t30S, t30E, t30E, now, t30S, t30E  // T30
        );

        try {
            // from 时间需覆盖 T-31
            GetLogsRequest request = new GetLogsRequest(PROJECT, LOGSTORE, (int)t30S, (int)now, "", query);
            GetLogsResponse response = client.GetLogs(request);
            List<QueriedLog> logs = response.getLogs();

            if (logs != null && !logs.isEmpty()) {
                String t1a="0", t1s="0", t7a="0", t7s="0", t15a="0", t15s="0", t30a="0", t30s="0";
                for (LogContent c : logs.get(0).mLogItem.mContents) {
                    if ("t1a".equals(c.mKey)) t1a = c.mValue;
                    else if ("t1s".equals(c.mKey)) t1s = c.mValue;
                    else if ("t7a".equals(c.mKey)) t7a = c.mValue;
                    else if ("t7s".equals(c.mKey)) t7s = c.mValue;
                    else if ("t15a".equals(c.mKey)) t15a = c.mValue;
                    else if ("t15s".equals(c.mKey)) t15s = c.mValue;
                    else if ("t30a".equals(c.mKey)) t30a = c.mValue;
                    else if ("t30s".equals(c.mKey)) t30s = c.mValue;
                }
                return String.format("%s,%s,%s,%s,%s,%s,%s,%s", t1a, t1s, t7a, t7s, t15a, t15s, t30a, t30s);
            }
        } catch (Exception e) {
            log.error("Query failed for " + date, e);
        }
        return "0,0,0,0,0,0,0,0";
    }
}