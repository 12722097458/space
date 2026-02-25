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
public class T1SlsDailyMetricsRunner {

    private static final String ENDPOINT = "cn-shanghai.log.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
    private static final String ACCESS_KEY_SECRET = "ACCESS_KEY_SECRET";
    private static final String PROJECT = "k8s-log-ca29bbf76013145ae80715e56ce1f199c";
    private static final String LOGSTORE = "event-log";

    public static void main(String[] args) {

        LocalDate startDate = LocalDate.of(2026, 2, 14);
        LocalDate endDate = LocalDate.now();

        String fileName = "t1.csv";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {

            writer.write("日期,注册数,日活,访问量,周活,月活,新客留存,老客留存,流失回流,T0创建,T7创建,工作流创建人数,工作流运行人数,工作流发布人数,工作流订阅人数,因子应用人数,策略应用人数");
            writer.newLine();

            LocalDate current = startDate;
            while (!current.isAfter(endDate)) {

                System.out.println("统计中: " + current);

                long dayStart = current.minusDays(1).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
                long dayEnd   = current.atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

                long weekStart  = current.minusWeeks(1).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
                long monthStart = current.minusMonths(1).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

                long preStart = current.minusDays(2).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
                long preEnd   = dayStart;

                long t7SignupStart = current.minusDays(8).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
                long t7SignupEnd   = current.minusDays(7).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

                String sql = buildSql(
                        dayStart, dayEnd,
                        weekStart, monthStart,
                        preStart, preEnd,
                        t7SignupStart, t7SignupEnd
                );

                Map<String, String> result = executeQuery(sql, (int) monthStart, (int) dayEnd);

                LocalDate bizDate = current.minusDays(1);

                writer.write(String.join(",",
                        bizDate.toString(),
                        get(result, "signup_count"),
                        get(result, "daily_active_count"),
                        get(result, "ip_set"),
                        get(result, "weekly_active_count"),
                        get(result, "monthly_active_count"),
                        get(result, "new_customer_active"),
                        get(result, "old_customer_active"),
                        get(result, "customer_return"),
                        get(result, "create_workflow_t0"),
                        get(result, "create_workflow_t7"),
                        get(result, "workflow_create_user_count"),
                        get(result, "workflow_run_user_count"),
                        get(result, "workflow_publish_user_count"),
                        get(result, "workflow_subscribe_user_count"),
                        get(result, "super_chart_factor_user_count"),
                        get(result, "super_chart_backtest_user_count")
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

    private static String buildSql(long dayStart, long dayEnd,
                                   long weekStart, long monthStart,
                                   long preStart, long preEnd,
                                   long t7SignupStart, long t7SignupEnd) {

        return String.format(
                "* | select " +
                "t1.signup_count, t2.daily_active_count, t_pv.ip_set, " +
                "t3.weekly_active_count, t4.monthly_active_count, " +
                "t5.new_customer_active, t6.old_customer_active, t7.customer_return, " +
                "t8.create_workflow_t0, t9.create_workflow_t7, " +
                "t10.workflow_create_user_count, t11.workflow_run_user_count, " +
                "t14.workflow_publish_user_count, t15.workflow_subscribe_user_count, " +
                "t16.super_chart_factor_user_count, t17.super_chart_backtest_user_count " +
                "FROM " +
                "(SELECT approx_distinct(user_id) signup_count FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) t1, " +
                "(SELECT approx_distinct(user_id) daily_active_count FROM log WHERE user_id IS NOT NULL AND __time__ >= %d AND __time__ < %d) t2, " +
                "(SELECT count(distinct(concat(ua,'-',ip))) ip_set FROM log WHERE __time__ >= %d AND __time__ < %d) t_pv, " +
                "(SELECT count(distinct user_id) weekly_active_count FROM log WHERE user_id IS NOT NULL AND __time__ >= %d AND __time__ < %d) t3, " +
                "(SELECT count(distinct user_id) monthly_active_count FROM log WHERE user_id IS NOT NULL AND __time__ >= %d AND __time__ < %d) t4, " +
                "(SELECT approx_distinct(user_id) new_customer_active FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) AND __time__ >= %d AND __time__ < %d) t5, " +
                "(SELECT approx_distinct(user_id) old_customer_active FROM log WHERE user_id IN (SELECT user_id FROM log WHERE __time__ >= %d AND __time__ < %d AND user_id NOT IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d)) AND __time__ >= %d AND __time__ < %d) t6, " +
                "(SELECT approx_distinct(user_id) customer_return FROM log WHERE __time__ >= %d AND __time__ < %d AND user_id NOT IN (SELECT distinct user_id FROM log WHERE __time__ >= %d AND __time__ < %d UNION ALL SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d)) t7, " +
                "(SELECT approx_distinct(user_id) create_workflow_t0 FROM log WHERE event='workflow_create' AND user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) AND __time__ >= %d AND __time__ < %d) t8, " +
                "(SELECT count(distinct user_id) create_workflow_t7 FROM log WHERE event='workflow_create' AND user_id IN (SELECT user_id FROM log WHERE event='user_signup' AND __time__ >= %d AND __time__ < %d) AND __time__ >= %d AND __time__ < %d) t9, " +
                "(SELECT approx_distinct(user_id) workflow_create_user_count FROM log WHERE event='workflow_create' AND __time__ >= %d AND __time__ < %d) t10, " +
                "(SELECT approx_distinct(user_id) workflow_run_user_count FROM log WHERE event='workflow_run' AND __time__ >= %d AND __time__ < %d) t11, " +
                "(SELECT approx_distinct(user_id) workflow_publish_user_count FROM log WHERE event='workflow_publish' AND __time__ >= %d AND __time__ < %d) t14, " +
                "(SELECT approx_distinct(user_id) workflow_subscribe_user_count FROM log WHERE event='workflow_subscribe' AND __time__ >= %d AND __time__ < %d) t15, " +
                "(SELECT approx_distinct(user_id) super_chart_factor_user_count FROM log WHERE event='super_chart' AND json_extract_scalar(props,'$.feature_tag')='factor' AND __time__ >= %d AND __time__ < %d) t16, " +
                "(SELECT approx_distinct(user_id) super_chart_backtest_user_count FROM log WHERE event='super_chart' AND json_extract_scalar(props,'$.feature_tag')='backtest' AND __time__ >= %d AND __time__ < %d) t17",
                dayStart, dayEnd,
                dayStart, dayEnd,
                dayStart, dayEnd,
                weekStart, dayEnd,
                monthStart, dayEnd,
                preStart, preEnd, dayStart, dayEnd,
                preStart, preEnd, preStart, preEnd, dayStart, dayEnd,
                dayStart, dayEnd, preStart, preEnd, dayStart, dayEnd,
                dayStart, dayEnd, dayStart, dayEnd,
                t7SignupStart, t7SignupEnd, currentMinus7(dayEnd), dayEnd,
                dayStart, dayEnd,
                dayStart, dayEnd,
                dayStart, dayEnd,
                dayStart, dayEnd,
                dayStart, dayEnd,
                dayStart, dayEnd
        );
    }

    private static long currentMinus7(long dayEnd) {
        return dayEnd - 7 * 86400;
    }

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