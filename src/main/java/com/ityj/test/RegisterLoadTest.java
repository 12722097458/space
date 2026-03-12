package com.ityj.test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/*压测*/
public class RegisterLoadTest {

    static final String DEFAULT_URL = "http://192.168.1.3:8787/auth/register";
    static final int N = 100;              // id: 0..99
    static final int CONCURRENCY = 100;    // concurrent requests
    static final Duration REQ_TIMEOUT = Duration.ofSeconds(60);

    public static void main(String[] args) throws Exception {
        String url = args != null && args.length > 0 && args[0] != null && !args[0].isBlank() ? args[0] : DEFAULT_URL;
        System.out.println("Target URL: " + url + " " + new Date());
        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        List<CompletableFuture<Result>> futures = new ArrayList<>(N);
        long suiteStart = System.nanoTime();

        for (int id = 0; id < N; id++) {
            final int finalId = id;
            futures.add(CompletableFuture.supplyAsync(() -> callOnce(client, url, finalId), pool));
        }

        List<Result> results = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        long suiteMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - suiteStart);

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        System.out.println("end" + new Date());

        printSummary(results, suiteMs);

    }

    static Result callOnce(HttpClient client, String url, int id) {
        String userName = "test-Asdd22" + id;
        String email = "t29xizdvuc" + id + "@mx.tempmail.cn";

        String json = """
                {
                  "registerType": "email",
                  "userName": "%s",
                  "password": "47EC2DD791E31E2EF2076CAF64ED9B3D",
                  "email": "%s",
                  "verificationCode": "999999",
                  "regionName": "AF",
                  "language": 3
                }
                """.formatted(userName, email);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(REQ_TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        long start = System.nanoTime();
        try {
            System.out.printf("start id=%d userName=%s%n", id, userName);
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.printf("done  id=%d ms=%d status=%d%n", id, ms, resp.statusCode());
            return new Result(id, ms, resp.statusCode(), null, resp.body());
        } catch (Exception e) {
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.printf("fail  id=%d ms=%d err=%s%n", id, ms, e);
            return new Result(id, ms, -1, e.toString(), null);
        }
    }

    static void printSummary(List<Result> results, long suiteMs) {
        int ok = 0;
        int fail = 0;
        List<Long> times = new ArrayList<>(results.size());

        for (Result r : results) {
            times.add(r.ms);
            if (r.err != null || r.status < 200 || r.status >= 300) {
                fail++;
            } else {
                ok++;
            }
        }

        times.sort(Comparator.naturalOrder());

        System.out.println("==== Summary ====");
        System.out.println("Total requests: " + results.size());
        System.out.println("OK: " + ok + ", Fail: " + fail);
        System.out.println("Suite time (ms): " + suiteMs);
        System.out.println("p50(ms): " + percentile(times, 50));
        System.out.println("p90(ms): " + percentile(times, 90));
        System.out.println("p95(ms): " + percentile(times, 95));
        System.out.println("p99(ms): " + percentile(times, 99));
        System.out.println("max(ms): " + (times.isEmpty() ? 0 : times.get(times.size() - 1)));

        results.sort(Comparator.comparingLong((Result r) -> r.ms).reversed());
        System.out.println();
        System.out.println("==== Slowest 10 ====");
        for (int i = 0; i < Math.min(10, results.size()); i++) {
            Result r = results.get(i);
            System.out.printf("id=%d ms=%d status=%d err=%s%n", r.id, r.ms, r.status, r.err);
        }
    }

    static long percentile(List<Long> sorted, int p) {
        if (sorted.isEmpty()) {
            return 0;
        }
        double rank = (p / 100.0) * (sorted.size() - 1);
        int lo = (int) Math.floor(rank);
        int hi = (int) Math.ceil(rank);
        if (lo == hi) {
            return sorted.get(lo);
        }
        double frac = rank - lo;
        return Math.round(sorted.get(lo) * (1 - frac) + sorted.get(hi) * frac);
    }

    record Result(int id, long ms, int status, String err, String body) {}
}

