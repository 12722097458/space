package com.ityj.feishu.bitable.schedule;

import com.ityj.feishu.bitable.sync.FeishuBitableSyncService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Map;

@Slf4j
@Component
public class FeishuBitableDailySyncScheduler {

    private static final ZoneId ZONE_BEIJING = ZoneId.of("Asia/Shanghai");

    private final FeishuBitableSyncService syncService;

    public FeishuBitableDailySyncScheduler(FeishuBitableSyncService syncService) {
        this.syncService = syncService;
    }

    /**
     * 北京时间每天早上 8 点，执行“前一天”数据同步。
     */
    @Scheduled(cron = "0 0 8 * * *", zone = "Asia/Shanghai")
    public void syncYesterdayAtMorning() {
        LocalDate yesterday = LocalDate.now(ZONE_BEIJING).minusDays(1);
        String dateStr = yesterday.toString();
        log.info("[feishu-bitable-scheduler] start daily sync, date={}", dateStr);
        Map<String, Object> result = syncService.syncAllDailyByDate(dateStr);
        log.info("[feishu-bitable-scheduler] finished daily sync, result={}", result);
    }
}
