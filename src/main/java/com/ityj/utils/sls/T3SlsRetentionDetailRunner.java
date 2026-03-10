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
//    private static final String EXCLUDED_USER_IDS = "43243,43241,43239,43238,43237,43233,43228,43223,43222,43221,43220,43218,43217,43210,43208,43204,43202,43196,43193,25473,43189,43188,43186,43184,43180,43176,43173,43170,43169,43165,43163,43162,43157,43149,43148,43145,43144,43143,43142,43135,43134,23683,43131,43129,43125,43123,43122,43118,43114,43112,43110,43109,43108,43107,43105,43104,43102,43100,43099,25151,43098,43094,43093,43092,43091,43089,43088,43087,43082,43080,43078,43077,43076,24377,43068,43066,43065,43064,43061,43058,43054,24387,43050,13734,43044,43042,43041,43039,43037,43036,43035,43034,43033,43032,43030,43029,43027,43024,43023,43021,43020,43017,43016,43015,24427,43012,43007,43006,43002,25024,43000,42998,42996,42995,42992,42991,42989,42986,42982,42975,42974,42972,42969,42966,42965,42963,42961,42960,42959,42958,42954,42952,42951,42948,42947,23753,42943,42942,42941,42939,42934,42933,42931,42930,42929,42928,42927,42926,42925,42921,42918,42917,42912,42911,42910,42909,42908,42902,42901,42900,42899,42895,42893,42891,42890,42888,42885,42884,42883,42877,42876,42874,42872,42871,42870,42869,42868,42867,12818,42855,42849,42848,42843,42842,42840,42838,42831,42830,42828,42825,42822,42821,42820,42817,42813,42811,42810,42805,42803,42800,42799,42798,42794,42793,42792,42791,42789,42786,42785,42784,42780,42776,42775,42772,42771,42766,42760,42756,42755,42754,42752,42751,42750,42734,42731,42729,42723,42722,42721,42720,42719,42718,42717,42716,42715,42714,42709,42707,42705,42704,42703,42702,42701,42700,24022,42696,42695,42694,42693,42692,42689,42688,42684,42683,42682,42681,42680,42677,42675,42673,42671,42670,42669,42667,42666,23844,42663,42662,42661,42660,42658,42656,42654,42651,42650,14504,42648,42644,42639,42638,42633,42632,42630,42629,42628,42627,42625,42623,42621,42620,42618,42617,42616,23169,42615,42613,42612,42611,42610,42609,42608,42607,42606,42605,42604,42603,42602,42601,42600,42583,42579,42576,42575,42574,42572,42571,42570,42569,42483,42478,42477,42476,42475,42473,42472,42471,42469,42467,42466,42460,42459,42458,42457,42456,42455,42454,42453,42450,42449,42447,42446,42445,24050,42444,42443,42441,42439,42438,42435,42434,42433,42432,42431,42429,42427,42426,42425,42422,42421,42420,42418,42417,42413,42412,42410,42409,42407,42406,42405,42404,42403,42401,42398,42397,42396,42394,42393,24467,42392,42391,42389,42388,42387,42386,42385,42384,42382,42381,42379,42378,42377,42376,42375,42374,42371,23115,42369,42368,42367,42366,42364,42361,42359,42358,42357,42356,42355,42354,42353,42352,42351,42350,42349,42348,42347,42342,42341,42339,42338,42337,42334,42332,42331,42330,42329,42328,42327,42326,42323,42322,42321,42320,42319,42318,42317,42316,42315,42314,42312,42311,42309,42306,42305,42304,42303,42302,42301,42300,42297,42296,42294,42293,42292,42291,42290,42289,42288,42287,42286,42285,42284,42281,42280,42279,42278,42277,42276,42275,42274,42273,42271,42270,24677,42268,42267,42264,42263,42261,42260,42259,42258,42256,42251,42249,42248,42247,42246,42245,42244,42243,42242,42241,42239,42233,42232,42230,22883,12678,12698,42160";
private static final String EXCLUDED_USER_IDS ="";
    private static final String EXCLUDE_CONDITION_TEMPLATE = "user_id NOT IN (%s)";


    public static void main(String[] args) {

        LocalDate startDate = LocalDate.of(2026, 3, 1);
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

    private static String buildExcludeCondition() {
        StringBuilder quotedIds = new StringBuilder();
        String[] ids = EXCLUDED_USER_IDS.split(",");
        for (int i = 0; i < ids.length; i++) {
            if (i > 0) quotedIds.append(",");
            quotedIds.append("'").append(ids[i].trim()).append("'");
        }
        return String.format(EXCLUDE_CONDITION_TEMPLATE, quotedIds);
    }

    private static String buildSql(
            long dayStart, long dayEnd,
            long t1SignupStart, long t1SignupEnd,
            long t7SignupStart, long t7SignupEnd,
            long t15SignupStart, long t15SignupEnd,
            long t30SignupStart, long t30SignupEnd) {

        String excludeCondition = buildExcludeCondition();

        return String.format(
                "* | SELECT " +
                "t1.signup_active_user_count t1_active, t2.signup_user_count t1_signup, " +
                "t3.signup_active_user_count t7_active, t4.signup_user_count t7_signup, " +
                "t5.signup_active_user_count t15_active, t6.signup_user_count t15_signup, " +
                "t7.signup_active_user_count t30_active, t8.signup_user_count t30_signup " +
                "FROM " +
                "(SELECT APPROX_DISTINCT(user_id) signup_active_user_count FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) t1, " +
                "(SELECT APPROX_DISTINCT(user_id) signup_user_count FROM log WHERE event='user_signup' AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) t2, " +
                "(SELECT COUNT(DISTINCT user_id) signup_active_user_count FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) t3, " +
                "(SELECT COUNT(DISTINCT user_id) signup_user_count FROM log WHERE event='user_signup' AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) t4, " +
                "(SELECT COUNT(DISTINCT user_id) signup_active_user_count FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) t5, " +
                "(SELECT COUNT(DISTINCT user_id) signup_user_count FROM log WHERE event='user_signup' AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) t6, " +
                "(SELECT COUNT(DISTINCT user_id) signup_active_user_count FROM log WHERE user_id IN (SELECT distinct user_id FROM log WHERE event='user_signup' AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) t7, " +
                "(SELECT COUNT(DISTINCT user_id) signup_user_count FROM log WHERE event='user_signup' AND " + excludeCondition + " AND __time__ >= %d AND __time__ < %d) t8",

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