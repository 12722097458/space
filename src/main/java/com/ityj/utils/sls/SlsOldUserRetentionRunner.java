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
public class SlsOldUserRetentionRunner {

    private static final String ENDPOINT = "cn-shanghai.log.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
    private static final String ACCESS_KEY_SECRET = "ACCESS_KEY_SECRET";
    private static final String PROJECT = "k8s-log-ca29bbf76013145ae80715e56ce1f199c";
    private static final String LOGSTORE = "event-log";

    private static final String INTERNAL_USERS =
            "'37125','36976','36651','36563','35981','34945','34932','34482','34329','34222'," +
            "'33788','32839','32197','30363','30139','29985','29967','29597','28704','27906'," +
            "'26402','22383','19887','19861','18327','17467','17121','17010','16875','16544'," +
            "'16216','15721','15674','14661','12525','10531','10428','10393','10010','10007'," +
            "'10002','10001','4','3','2'";

    public static void main(String[] args) {

        LocalDate startDate = LocalDate.of(2025, 11, 13);
        LocalDate endDate = LocalDate.now();

        String fileName = "老客留存统计.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {

            writer.write("日期,老客留存人数");
            writer.newLine();

            LocalDate current = startDate;
            while (!current.isAfter(endDate)) {

                System.out.println("正在统计: " + current);

                long preStart = current.minusDays(2).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
                long preEnd   = current.minusDays(1).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

                long yesStart = current.minusDays(1).atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));
                long yesEnd   = current.atStartOfDay().toEpochSecond(ZoneOffset.ofHours(8));

                long queryStart = preStart;
                long queryEnd = yesEnd;

                String sql = buildSql(preStart, preEnd, yesStart, yesEnd);

                long count = executeQuery(sql, queryStart, queryEnd);

                LocalDate businessDate = current.minusDays(1);

                writer.write(businessDate + "," + count);
                writer.newLine();
                writer.flush();

                current = current.plusDays(1);
            }

            System.out.println("老客留存统计完成，文件已生成：" + fileName);

        } catch (Exception e) {
            log.error("批量任务异常", e);
        }
    }

    private static String buildSql(long preStart, long preEnd, long yesStart, long yesEnd) {

        return String.format(
                "* | SELECT COUNT(DISTINCT user_id) AS cnt " +
                "FROM log WHERE user_id IS NOT NULL " +
                "AND user_id NOT IN (%s) " +
                "AND user_id IN ( " +
                "   SELECT user_id FROM log WHERE user_id IS NOT NULL " +
                "   AND user_id NOT IN (%s) " +
                "   AND __time__ >= %d AND __time__ < %d " +
                "   AND user_id NOT IN ( " +
                "       SELECT DISTINCT user_id FROM log WHERE user_id IS NOT NULL " +
                "       AND event='user_signup' AND __time__ >= %d AND __time__ < %d " +
                "   ) " +
                ") " +
                "AND __time__ >= %d AND __time__ < %d",
                INTERNAL_USERS,
                INTERNAL_USERS,
                preStart, preEnd,
                preStart, preEnd,
                yesStart, yesEnd
        );
    }

    private static long executeQuery(String query, long start, long end) {

        Client client = new Client(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET);

        try {
            GetLogsRequest request =
                    new GetLogsRequest(PROJECT, LOGSTORE, (int) start, (int) end, "", query);

            GetLogsResponse response = client.GetLogs(request);
            List<QueriedLog> logs = response.getLogs();

            if (logs != null && !logs.isEmpty()) {
                for (LogContent c : logs.get(0).mLogItem.mContents) {
                    if ("cnt".equals(c.mKey)) {
                        return Long.parseLong(c.mValue);
                    }
                }
            }
        } catch (Exception e) {
            log.error("查询失败", e);
        }
        return 0;
    }
}
