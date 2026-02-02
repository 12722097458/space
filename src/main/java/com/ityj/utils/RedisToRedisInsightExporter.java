package com.ityj.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisToRedisInsightExporter {

    private Jedis jedis;
    private ObjectMapper mapper;


    public RedisToRedisInsightExporter(String host, int port, String password, int db) {
        this.jedis = new Jedis(host, port);
        this.mapper = new ObjectMapper();
        if (password != null && !password.isEmpty()) {
            this.jedis.auth(password);
        }
        jedis.select(db); //默认db是0

    }

    /**
     * 导出指定模式的key为Redis Insight兼容的JSON格式
     */
    public void exportToRedisInsightFormat(String pattern, String outputFilePath) throws IOException {
        ArrayNode jsonArray = mapper.createArrayNode();
        String cursor = "0";
        ScanParams scanParams = new ScanParams().match(pattern).count(1000);

        do {
            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
            List<String> keys = scanResult.getResult();
            cursor = scanResult.getCursor();

            for (String key : keys) {
                try {
                    ObjectNode keyData = exportKeyData(key);
                    if (keyData != null) {
                        jsonArray.add(keyData);
                    }
                } catch (Exception e) {
                    System.err.println("导出键失败: " + key + ", 错误: " + e.getMessage());
                }
            }
        } while (!cursor.equals("0"));

        // 写入JSON文件
        try (FileWriter fileWriter = new FileWriter(outputFilePath)) {
            mapper.writerWithDefaultPrettyPrinter().writeValue(fileWriter, jsonArray);
        }

        System.out.println("成功导出 " + jsonArray.size() + " 个键到文件: " + outputFilePath);
    }

    /**
     * 导出单个键的数据
     */
    private ObjectNode exportKeyData(String key) {
        String type = jedis.type(key);
        ObjectNode keyNode = mapper.createObjectNode();
        
        keyNode.put("key", key);
        keyNode.put("type", type);
        
        // 获取TTL（剩余时间）
        long ttl = jedis.ttl(key);
        keyNode.put("ttl", ttl);

        // 根据数据类型处理值
        switch (type) {
            case "string":
                handleStringType(key, keyNode);
                break;
            case "hash":
                handleHashType(key, keyNode);
                break;
            case "list":
                handleListType(key, keyNode);
                break;
            case "set":
                handleSetType(key, keyNode);
                break;
            case "zset":
                handleZSetType(key, keyNode);
                break;
            default:
                System.out.println("不支持的数据类型: " + type + " for key: " + key);
                return null;
        }

        return keyNode;
    }

    private void handleStringType(String key, ObjectNode keyNode) {
        String value = jedis.get(key);
        keyNode.put("value", value);
    }

    private void handleHashType(String key, ObjectNode keyNode) {
        Map<String, String> hashData = jedis.hgetAll(key);
        ObjectNode valueNode = mapper.createObjectNode();
        
        for (Map.Entry<String, String> entry : hashData.entrySet()) {
            valueNode.put(entry.getKey(), entry.getValue());
        }
        
        keyNode.set("value", valueNode);
    }

    private void handleListType(String key, ObjectNode keyNode) {
        List<String> listData = jedis.lrange(key, 0, -1);
        ArrayNode valueNode = mapper.createArrayNode();
        
        for (String item : listData) {
            valueNode.add(item);
        }
        
        keyNode.set("value", valueNode);
    }

    private void handleSetType(String key, ObjectNode keyNode) {
        Set<String> setData = jedis.smembers(key);
        ArrayNode valueNode = mapper.createArrayNode();
        
        for (String item : setData) {
            valueNode.add(item);
        }
        
        keyNode.set("value", valueNode);
    }

    private void handleZSetType(String key, ObjectNode keyNode) {
        List<Tuple> zsetData = jedis.zrangeWithScores(key, 0, -1);
        ArrayNode valueNode = mapper.createArrayNode();
        
        for (Tuple tuple : zsetData) {
            ObjectNode memberNode = mapper.createObjectNode();
            memberNode.put("score", tuple.getScore());
            memberNode.put("value", tuple.getElement());
            valueNode.add(memberNode);
        }
        
        keyNode.set("value", valueNode);
    }

    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }

    public static void main(String[] args) {
        RedisToRedisInsightExporter exporter = null;
        try {
            // 配置Redis连接
//            exporter = new RedisToRedisInsightExporter("47.116.215.155", 6379, "rKj8nvJKhMYE3HP4BqTy", 1);
            exporter = new RedisToRedisInsightExporter("192.168.1.3", 6380, "", 0);

            // 导出指定模式的键（例如：user:*）
            exporter.exportToRedisInsightFormat("stock_tick_*", "stock_tick_prod_db1.json");
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (exporter != null) {
                exporter.close();
            }
        }
    }
}