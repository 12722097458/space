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
public class T201SlsWorkflowCoreMetricsRunner {

    private static final String ENDPOINT = "cn-shanghai.log.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
    private static final String ACCESS_KEY_SECRET = "ACCESS_KEY_SECRET";
    private static final String PROJECT = "k8s-log-ca29bbf76013145ae80715e56ce1f199c";
    private static final String LOGSTORE = "event-log";


    public static void main(String[] args) {
        LocalDate startDate = LocalDate.of(2026, 2, 14);
        LocalDate endDate = LocalDate.now();

        String fileName = "t2_01.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {

            writer.write("日期,工作流创建率,工作流运行率,工作流运行成功率,工作流发布率,工作流订阅率,超级图表因子应用率,超级图表策略应用率");
            writer.newLine();

            LocalDate current = startDate;
            while (!current.isAfter(endDate)) {

                log.info("统计日期: {}", current);
                String metrics = queryMetrics(current);

                // 业务日期 = 统计日前一天
                LocalDate bizDate = current.minusDays(1);

                writer.write(bizDate + "," + metrics);
                writer.newLine();
                writer.flush();

                current = current.plusDays(1);
            }

            log.info("统计完成，结果写入 {}", fileName);
        } catch (Exception e) {
            log.error("执行失败", e);
        }
    }

    public static String queryMetrics(LocalDate baseDate) {
        Client client = new Client(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET);

        long dayStart = baseDate.minusDays(1)
                .atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
        long dayEnd = baseDate
                .atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

        String query = String.format(
            "* | SELECT " +
            "ROUND(t1.cnt * 100.0 / NULLIF(t2.cnt,0),2) as v1 , " +
            "ROUND(t3.cnt * 100.0 / NULLIF(t2.cnt,1),2) as v2 , " +
            "ROUND(t4.cnt * 100.0 / NULLIF(t5.cnt,1),2) as v3 , " +
            "ROUND(t6.cnt * 100.0 / NULLIF(t2.cnt,1),2) as v4 , " +
            "ROUND(t7.cnt * 100.0 / NULLIF(t2.cnt,1),2) as v5 , " +
            "ROUND(t8.cnt * 100.0 / NULLIF(t2.cnt,1),2) as v6 , " +
            "ROUND(t9.cnt * 100.0 / NULLIF(t2.cnt,1),2) as v7  " +

            "FROM " +
            "(SELECT APPROX_DISTINCT(user_id) cnt FROM log WHERE event='workflow_create' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t1," +
            "(SELECT APPROX_DISTINCT(user_id) cnt FROM log WHERE user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t2," +
            "(SELECT APPROX_DISTINCT(user_id) cnt FROM log WHERE event='workflow_run' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t3," +
            "(SELECT COUNT(*) cnt FROM log WHERE event='workflow_run' AND status=201 AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t4," +
            "(SELECT COUNT(*) cnt FROM log WHERE event='workflow_run' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t5," +
            "(SELECT APPROX_DISTINCT(user_id) cnt FROM log WHERE event='workflow_publish' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t6," +
            "(SELECT APPROX_DISTINCT(user_id) cnt FROM log WHERE event='workflow_subscribe' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t7," +
            "(SELECT APPROX_DISTINCT(user_id) cnt FROM log WHERE event='super_chart' AND json_extract_scalar(props,'$.feature_tag')='factor' AND json_extract_scalar(props,'$.locator')='task_id' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t8," +
            "(SELECT APPROX_DISTINCT(user_id) cnt FROM log WHERE event='super_chart' AND json_extract_scalar(props,'$.feature_tag')='backtest' AND json_extract_scalar(props,'$.locator')='backtest_id' AND user_id IS NOT NULL AND __time__>=%d AND __time__<%d) t9",

            dayStart, dayEnd,
            dayStart, dayEnd,
            dayStart, dayEnd,
            dayStart, dayEnd,
            dayStart, dayEnd,
            dayStart, dayEnd,
            dayStart, dayEnd,
            dayStart, dayEnd,
            dayStart, dayEnd
        );

        try {
            GetLogsRequest request = new GetLogsRequest(
                    PROJECT, LOGSTORE, (int) dayStart, (int) dayEnd, "", query);

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
                        map.getOrDefault("v5", "0") + "," +
                        map.getOrDefault("v6", "0") + "," +
                        map.getOrDefault("v7", "0");
            }
        } catch (Exception e) {
            log.error("Query error: {}", baseDate, e);
        }

        return "0,0,0,0,0,0,0";
    }
}