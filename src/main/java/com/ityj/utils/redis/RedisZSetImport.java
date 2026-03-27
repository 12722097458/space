package com.ityj.utils.redis;

import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class RedisZSetImport {

    public static void main(String[] args) {
        // 连接 Redis
        Jedis jedis = new Jedis("localhost", 6379);

        String key = "tqx_leaderboard:realtime:L";

        // 构造数据
        Map<String, Double> data = new HashMap<>();
        data.put("100140:agent_1012", 91.0);
        data.put("100148:agent_1020", 92.0);
//        data.put("100146:agent_018", 93.0);
//        data.put("100144:agent_016", 94.0);
//        data.put("100141:agent_013", 95.0);
//        data.put("100139:agent_011", 96.0);
//        data.put("100142:agent_014", 97.0);
//        data.put("100145:agent_017", 98.0);
//        data.put("100143:agent_015", 99.0);
//        data.put("100147:agent_019", 100.0);

        // 批量插入
        jedis.zadd(key, data);

        System.out.println("导入完成");

        jedis.close();
    }
}