package com.ityj.feishu.bitable.controller;

import com.ityj.feishu.bitable.sync.FeishuBitableSyncService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 飞书多维表格同步接口（GET）。
 * <p>
 * 注意：同步过程可能较慢，可能导致调用方超时；若需要异步可后续再改造。
 */
@RestController
@RequestMapping("/feishu-bitable")
public class FeishuBitableController {

    private static final ZoneId ZONE_BEIJING = ZoneId.of("Asia/Shanghai");

    private static final int DEFAULT_RETENTION_START_DAYS_AGO = 7;
    private static final int DEFAULT_RETENTION_END_DAYS_AGO = 1;

    private final FeishuBitableSyncService syncService;

    public FeishuBitableController(FeishuBitableSyncService syncService) {
        this.syncService = syncService;
    }

    /**
     * 示例：
     * <pre>
     * GET /feishu-bitable/sync?profileKey=default&startTime=2026-03-20&endTime=2026-03-26
     * GET /feishu-bitable/sync?profileKey=channel-a
     * </pre>
     */
    @GetMapping("/sync")
    public ResponseEntity<Map<String, Object>> sync(
            @RequestParam(defaultValue = "default") String profileKey,
            @RequestParam(required = false) String startTime,
            @RequestParam(required = false) String endTime) {

        String resolvedStart = startTime != null ? startTime : defaultRetentionStartDateBeijing();
        String resolvedEnd = endTime != null ? endTime : defaultRetentionEndDateBeijing();

        boolean hasStart = startTime != null;
        boolean hasEnd = endTime != null;
        if (hasStart != hasEnd) {
            return ResponseEntity.badRequest().body(Map.of(
                    "code", -1,
                    "msg", "startTime 与 endTime 必须同时提供，或都不提供（自动按北京时间：开始=今天−7 天，结束=今天−1 天）"
            ));
        }

        syncService.sync(profileKey, resolvedStart, resolvedEnd);

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("code", 0);
        resp.put("msg", "sync completed");
        resp.put("profileKey", profileKey);
        resp.put("startTime", resolvedStart);
        resp.put("endTime", resolvedEnd);
        return ResponseEntity.ok(resp);
    }

    private static String defaultRetentionStartDateBeijing() {
        return LocalDate.now(ZONE_BEIJING)
                .minusDays(DEFAULT_RETENTION_START_DAYS_AGO)
                .toString();
    }

    private static String defaultRetentionEndDateBeijing() {
        return LocalDate.now(ZONE_BEIJING)
                .minusDays(DEFAULT_RETENTION_END_DAYS_AGO)
                .toString();
    }
}

