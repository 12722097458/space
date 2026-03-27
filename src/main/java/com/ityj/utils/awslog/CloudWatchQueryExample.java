package com.ityj.utils.awslog;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class CloudWatchQueryExample {

    private static final String ACCESS_KEY = System.getenv("AWS_ACCESS_KEY_ID");
    private static final String SECRET_KEY = System.getenv("AWS_SECRET_ACCESS_KEY");

    private static final Region REGION = Region.AP_EAST_1;

    private static final String LOG_GROUP = "/aws/containerinsights/tqx/application";

    public static void main(String[] args) throws Exception {
        CloudWatchLogsClient client = CloudWatchLogsClient.builder()
                .region(REGION)
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .build();

        // 查询语句（CloudWatch Logs Insights）
//        String query = """
//                fields ua, ip
//                | stats count_distinct(concat(ua,"-",ip)) as visitCount
//                | sort user_id
//                """;

        String query = """
                fields user_id
                | filter type="event" and user_id != "" and ispresent(user_id)
                | stats latest(user_id) as user_id by user_id
                | sort user_id
                """;


        ZoneId zone = ZoneId.of("Asia/Shanghai");

        LocalDate date = LocalDate.of(2026, 3, 12);

        long startTime = date.atStartOfDay(zone).toEpochSecond();
        long endTime = date.plusDays(1).atStartOfDay(zone).toEpochSecond() - 1;

        StartQueryRequest startQueryRequest = StartQueryRequest.builder()
                .logGroupName(LOG_GROUP)
                .startTime(startTime)
                .endTime(endTime)
                .queryString(query)
                .limit(1000)
                .build();

        StartQueryResponse startQueryResponse = client.startQuery(startQueryRequest);

        String queryId = startQueryResponse.queryId();

        GetQueryResultsResponse results;

        while (true) {

            Thread.sleep(2000);

            results = client.getQueryResults(
                    GetQueryResultsRequest.builder()
                            .queryId(queryId)
                            .build());

            if (results.status() == QueryStatus.COMPLETE) {
                break;
            }

            if (results.status() == QueryStatus.FAILED) {
                throw new RuntimeException("Query failed");
            }
        }

        for (List<ResultField> row : results.results()) {

            Map<String, String> map = new HashMap<>();

            for (ResultField field : row) {
                map.put(field.field(), field.value());
            }

            System.out.println(map);
        }

        client.close();
    }
}