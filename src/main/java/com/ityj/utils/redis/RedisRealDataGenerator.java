package com.ityj.utils.redis;

import redis.clients.jedis.Jedis;
import java.util.*;

public class RedisRealDataGenerator {

    private static final String[] FIRST_NAMES = {
            "Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Jamie", "Skyler",
            "Cameron", "Drew", "Hunter", "Quinn", "Avery", "Logan", "Peyton", "Reese",
            "Blake", "Parker", "Hayden", "Emerson", "Charlie", "Dakota", "Phoenix", "River"
    };

    private static final String[] LAST_NAMES = {
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia",
            "Rodriguez", "Wilson", "Martinez", "Anderson", "Taylor", "Thomas", "Hernandez"
    };

    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("192.168.1.3", 6380)) {
            System.out.println("🚀 正在注入使用 OSS 指定头像的数据...");

            Map<String, List<String>> leaderboardMap = new LinkedHashMap<>();
            // L榜
            leaderboardMap.put("100129", Arrays.asList("agent_101", "agent_102", "agent_103", "agent_104", "agent_105", "agent_001"));
            leaderboardMap.put("100134", Collections.singletonList("agent_006"));
            leaderboardMap.put("100130", Collections.singletonList("agent_002"));
            leaderboardMap.put("100131", Collections.singletonList("agent_003"));
            leaderboardMap.put("100136", Collections.singletonList("agent_008"));
            leaderboardMap.put("100132", Collections.singletonList("agent_004"));
            leaderboardMap.put("100138", Collections.singletonList("agent_010"));
            leaderboardMap.put("100135", Collections.singletonList("agent_007"));
            leaderboardMap.put("100133", Collections.singletonList("agent_005"));
            leaderboardMap.put("100137", Collections.singletonList("agent_009"));
            // S榜
            leaderboardMap.put("100146", Arrays.asList("agent_121", "agent_122", "agent_123", "agent_018"));
            leaderboardMap.put("100140", Collections.singletonList("agent_012"));
            leaderboardMap.put("100139", Collections.singletonList("agent_011"));
            leaderboardMap.put("100142", Collections.singletonList("agent_014"));
            leaderboardMap.put("100144", Collections.singletonList("agent_016"));
            leaderboardMap.put("100148", Collections.singletonList("agent_020"));
            leaderboardMap.put("100141", Collections.singletonList("agent_013"));
            leaderboardMap.put("100147", Collections.singletonList("agent_019"));
            leaderboardMap.put("100143", Collections.singletonList("agent_015"));
            leaderboardMap.put("100145", Collections.singletonList("agent_017"));

            Random random = new Random();
            int total = 0;
            // 图片总数
            int avatarCount = 20;

            for (Map.Entry<String, List<String>> entry : leaderboardMap.entrySet()) {
                String brokerId = entry.getKey();
                for (String agentId : entry.getValue()) {
                    String key = "tqx:agent:" + agentId;

                    // 1. 生成随机昵称
                    String nickname = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)] + " " +
                            LAST_NAMES[random.nextInt(LAST_NAMES.length)];

                    // 2. 核心修改：直接从 agentId 中提取数字部分 (例如 agent_001 -> 1)
                    // 使用 replaceAll 移除非数字字符，再转为 int 即可去掉前导零
                    int avatarIndex = Integer.parseInt(agentId.replaceAll("[^0-9]", ""));
                    String avatarUrl = "https://oss.pandaai.online/user/image/avatar/avatars-" + avatarIndex + ".png";

                    Map<String, String> fields = new HashMap<>();
                    fields.put("nickname", nickname);
                    fields.put("agentName", agentId);
                    fields.put("agentId", agentId);
                    fields.put("brokerAccountId", brokerId);
                    fields.put("avatarUrl", avatarUrl);
                    fields.put("status", "competing");

                    jedis.hset(key, fields);
                    System.out.println("Agent: " + agentId + " => Image: " + avatarIndex + ".png");
                    total++;
                }
            }
            System.out.println("✅ 注入完成，共 " + total + " 条。头像已设为 OSS http 地址。");
        }
    }
}