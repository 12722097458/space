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
public class T3SlsRetentionDetailRunner {

    private static final String ENDPOINT = "cn-shanghai.log.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
    private static final String ACCESS_KEY_SECRET = "ACCESS_KEY_SECRET";
    private static final String PROJECT = "k8s-log-ca29bbf76013145ae80715e56ce1f199c";
    private static final String LOGSTORE = "event-log";

    public static void main(String[] args) {

        LocalDate startDate = LocalDate.of(2026, 2, 14);
        LocalDate endDate = LocalDate.now();

        String fileName = "t3.csv";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {

            writer.write("日期,T1留存活跃数,T1留存注册数,T7留存活跃数,T7留存注册数,T15留存活跃数,T15留存注册数,T30留存活跃数,T30留存注册数");
            writer.newLine();

            LocalDate current = startDate;
            while (!current.isAfter(endDate)) {

                System.out.println("统计中: " + current);

                long dayStart = current.minusDays(1).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
                long dayEnd   = current.atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

                long t1SignupStart  = current.minusDays(2).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
                long t1SignupEnd    = dayStart;

                long t7SignupStart  = current.minusDays(8).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
                long t7SignupEnd    = current.minusDays(7).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

                long t15SignupStart = current.minusDays(16).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
                long t15SignupEnd   = current.minusDays(15).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

                long t30SignupStart = current.minusDays(31).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
                long t30SignupEnd   = current.minusDays(30).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

                String sql = buildSql(
                        dayStart, dayEnd,
                        t1SignupStart, t1SignupEnd,
                        t7SignupStart, t7SignupEnd,
                        t15SignupStart, t15SignupEnd,
                        t30SignupStart, t30SignupEnd
                );

                Map<String, String> result = executeQuery(sql, (int) t30SignupStart, (int) dayEnd);

                LocalDate bizDate = current.minusDays(1);

                writer.write(String.join(",",
                        bizDate.toString(),
                        get(result, "t1_active"),
                        get(result, "t1_signup"),
                        get(result, "t7_active"),
                        get(result, "t7_signup"),
                        get(result, "t15_active"),
                        get(result, "t15_signup"),
                        get(result, "t30_active"),
                        get(result, "t30_signup")
                ));
                writer.newLine();
                writer.flush();

                current = current.plusDays(1);
            }

            System.out.println("统计完成，文件生成：" + fileName);

        } catch (Exception e) {
            log.error("执行异常", e);
        }
    }

    private static String get(Map<String, String> map, String key) {
        return map.getOrDefault(key, "0");
    }

    private static String buildSql(
            long dayStart, long dayEnd,
            long t1SignupStart, long t1SignupEnd,
            long t7SignupStart, long t7SignupEnd,
            long t15SignupStart, long t15SignupEnd,
            long t30SignupStart, long t30SignupEnd) {

        return String.format(
                "* | SELECT " +
                "t1.signup_active_user_count t1_active, t2.signup_user_count t1_signup, " +
                "t3.signup_active_user_count t7_active, t4.signup_user_count t7_signup, " +
                "t5.signup_active_user_count t15_active, t6.signup_user_count t15_signup, " +
                "t7.signup_active_user_count t30_active, t8.signup_user_count t30_signup " +
                "FROM " +
                "(SELECT APPROX_DISTINCT(user_id) signup_active_user_count FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) AND __time__ >= %d AND __time__ < %d) t1, " +
                "(SELECT APPROX_DISTINCT(user_id) signup_user_count FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) t2, " +
                "(SELECT COUNT(DISTINCT user_id) signup_active_user_count FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) AND __time__ >= %d AND __time__ < %d) t3, " +
                "(SELECT COUNT(DISTINCT user_id) signup_user_count FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) t4, " +
                "(SELECT COUNT(DISTINCT user_id) signup_active_user_count FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) AND __time__ >= %d AND __time__ < %d) t5, " +
                "(SELECT COUNT(DISTINCT user_id) signup_user_count FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) t6, " +
                "(SELECT COUNT(DISTINCT user_id) signup_active_user_count FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) AND __time__ >= %d AND __time__ < %d) t7, " +
                "(SELECT COUNT(DISTINCT user_id) signup_user_count FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) t8",

                t1SignupStart, t1SignupEnd, dayStart, dayEnd,
                t1SignupStart, t1SignupEnd,

                t7SignupStart, t7SignupEnd, currentMinus7(dayEnd), dayEnd,
                t7SignupStart, t7SignupEnd,

                t15SignupStart, t15SignupEnd, currentMinus15(dayEnd), dayEnd,
                t15SignupStart, t15SignupEnd,

                t30SignupStart, t30SignupEnd, currentMinus30(dayEnd), dayEnd,
                t30SignupStart, t30SignupEnd
        );
    }

    private static long currentMinus7(long dayEnd) { return dayEnd - 7L * 86400; }
    private static long currentMinus15(long dayEnd) { return dayEnd - 15L * 86400; }
    private static long currentMinus30(long dayEnd) { return dayEnd - 30L * 86400; }

    private static Map<String, String> executeQuery(String query, int start, int end) {

        Client client = new Client(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET);
        Map<String, String> result = new HashMap<>();

        try {
            GetLogsRequest request =
                    new GetLogsRequest(PROJECT, LOGSTORE, start, end, "", query);

            GetLogsResponse response = client.GetLogs(request);
            List<QueriedLog> logs = response.getLogs();

            if (logs != null && !logs.isEmpty()) {
                for (LogContent c : logs.get(0).mLogItem.mContents) {
                    result.put(c.mKey, c.mValue);
                }
            }
        } catch (Exception e) {
            log.error("查询失败", e);
        }
        return result;
    }
}