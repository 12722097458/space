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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class T202SlsRetentionAndNewbieRunner {


    private static final String ENDPOINT = "cn-shanghai.log.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
    private static final String ACCESS_KEY_SECRET = "ACCESS_KEY_SECRET";
    private static final String PROJECT = "k8s-log-ca29bbf76013145ae80715e56ce1f199c";
    private static final String LOGSTORE = "event-log";

    public static void main(String[] args) {
        LocalDate startDate = LocalDate.of(2026, 2, 14);
        LocalDate endDate = LocalDate.now();

        String fileName = "t2_02.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {

            writer.write("日期,T1留存率,T7留存率,T15留存率,T30留存率,新手引导完成率");
            writer.newLine();

            LocalDate current = startDate;
            while (!current.isAfter(endDate)) {
                log.info("统计日期: {}", current);

                String result = queryMetrics(current);
                LocalDate bizDate = current.minusDays(1);

                writer.write(bizDate + "," + result);
                writer.newLine();
                writer.flush();

                current = current.plusDays(1);
            }

            log.info("统计完成，输出文件: {}", fileName);

        } catch (Exception e) {
            log.error("执行失败", e);
        }
    }

    public static String queryMetrics(LocalDate baseDate) {
        Client client = new Client(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET);

        long dayStart = baseDate.minusDays(1).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long dayEnd   = baseDate.atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

        long t1SignupStart  = baseDate.minusDays(2).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long t1SignupEnd    = baseDate.minusDays(1).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

        long t7SignupStart  = baseDate.minusDays(8).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long t7SignupEnd    = baseDate.minusDays(7).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

        long t15SignupStart = baseDate.minusDays(16).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long t15SignupEnd   = baseDate.minusDays(15).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

        long t30SignupStart = baseDate.minusDays(31).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long t30SignupEnd   = baseDate.minusDays(30).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

        String query = String.format(
            "* | SELECT " +
            "CASE WHEN t11.cnt=0 THEN 0 ELSE ROUND(t10.cnt*100.0/t11.cnt,2) END as v1, " +
            "CASE WHEN t13.cnt=0 THEN 0 ELSE ROUND(t12.cnt*100.0/t13.cnt,2) END as v2, " +
            "CASE WHEN t15.cnt=0 THEN 0 ELSE ROUND(t14.cnt*100.0/t15.cnt,2) END as v3, " +
            "CASE WHEN t17.cnt=0 THEN 0 ELSE ROUND(t16.cnt*100.0/t17.cnt,2) END as v4, " +
            "CASE WHEN t19.cnt=0 THEN 0 ELSE ROUND(t18.cnt*100.0/t19.cnt,2) END  as v5 " +

            "FROM " +

            // ---------- T1 ----------
            "(SELECT COUNT(DISTINCT l.user_id) cnt FROM log l JOIN " +
            " (SELECT DISTINCT user_id FROM log WHERE event='user_signup' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) u " +
            " ON l.user_id=u.user_id WHERE l.user_id IS NOT NULL AND l.__time__>=%d AND l.__time__<%d) t10," +

            "(SELECT APPROX_DISTINCT(user_id) cnt FROM log WHERE event='user_signup' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t11," +

            // ---------- T7 ----------
            "(SELECT COUNT(DISTINCT l.user_id) cnt FROM log l JOIN " +
            " (SELECT DISTINCT user_id FROM log WHERE event='user_signup' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) u " +
            " ON l.user_id=u.user_id WHERE l.user_id IS NOT NULL AND l.__time__>=%d AND l.__time__<%d) t12," +

            "(SELECT COUNT(DISTINCT user_id) cnt FROM log WHERE event='user_signup' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t13," +

            // ---------- T15 ----------
            "(SELECT COUNT(DISTINCT l.user_id) cnt FROM log l JOIN " +
            " (SELECT DISTINCT user_id FROM log WHERE event='user_signup' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) u " +
            " ON l.user_id=u.user_id WHERE l.user_id IS NOT NULL AND l.__time__>=%d AND l.__time__<%d) t14," +

            "(SELECT COUNT(DISTINCT user_id) cnt FROM log WHERE event='user_signup' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t15," +

            // ---------- T30 ----------
            "(SELECT COUNT(DISTINCT l.user_id) cnt FROM log l JOIN " +
            " (SELECT DISTINCT user_id FROM log WHERE event='user_signup' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) u " +
            " ON l.user_id=u.user_id WHERE l.user_id IS NOT NULL AND l.__time__>=%d AND l.__time__<%d) t16," +

            "(SELECT COUNT(DISTINCT user_id) cnt FROM log WHERE event='user_signup' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t17," +

            // ---------- 新手引导 ----------
            "(SELECT APPROX_DISTINCT(user_id) cnt FROM log WHERE route='/userTask/complete' AND status=200 AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t18," +
            "(SELECT APPROX_DISTINCT(user_id) cnt FROM log WHERE event='user_signup' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t19",

            // 参数绑定
            t1SignupStart, t1SignupEnd, dayStart, dayEnd,
            t1SignupStart, t1SignupEnd,

            t7SignupStart, t7SignupEnd, t7SignupEnd, dayEnd,
            t7SignupStart, t7SignupEnd,

            t15SignupStart, t15SignupEnd, t15SignupEnd, dayEnd,
            t15SignupStart, t15SignupEnd,

            t30SignupStart, t30SignupEnd, t30SignupEnd, dayEnd,
            t30SignupStart, t30SignupEnd,

            dayStart, dayEnd,
            dayStart, dayEnd
        );

        try {
            GetLogsRequest request = new GetLogsRequest(
                    PROJECT, LOGSTORE,
                    (int) t30SignupStart,
                    (int) dayEnd,
                    "", query
            );

            GetLogsResponse response = client.GetLogs(request);
            List<QueriedLog> logs = response.getLogs();

            if (logs != null && !logs.isEmpty()) {
                Map<String, String> map = new HashMap<>();
                for (LogContent c : logs.get(0).mLogItem.mContents) {
                    map.put(c.mKey, c.mValue);
                }

                return map.getOrDefault("v1", "0") + "," +
                        map.getOrDefault("v2", "0") + "," +
                        map.getOrDefault("v3", "0") + "," +
                        map.getOrDefault("v4", "0") + "," +
                        map.getOrDefault("v5", "0");
            }

        } catch (Exception e) {
            log.error("Query error: {}", baseDate, e);
        }

        return "0,0,0,0,0";
    }
}