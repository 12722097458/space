package com.ityj.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.File;
import java.util.*;

public class RedisDataImporter {
    
    private JedisPool jedisPool;
    private ObjectMapper objectMapper;
    
    public RedisDataImporter(String host, int port, String password) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(2);
        
        if (password != null && !password.isEmpty()) {
            this.jedisPool = new JedisPool(poolConfig, host, port, 2000, password);
        } else {
            this.jedisPool = new JedisPool(poolConfig, host, port, 2000);
        }
        
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * 从JSON文件导入数据到Redis（基础版本）
     */
    public ImportResult importFromJsonFile(String filePath, int batchSize) throws Exception {
        System.out.println("开始导入JSON文件: " + filePath);
        
        // 读取JSON文件
        List<RedisKeyData> keyDataList = readJsonFile(filePath);
        System.out.println("读取到 " + keyDataList.size() + " 条数据");
        
        return batchImportData(keyDataList, batchSize);
    }
    
    /**
     * 读取JSON文件并解析为Redis数据对象
     */
    private List<RedisKeyData> readJsonFile(String filePath) throws Exception {
        File file = new File(filePath);
        
        // 支持两种JSON格式：
        // 1. 数组格式: [{"key": "...", "type": "...", "value": "...", "ttl": ...}, ...]
        // 2. 对象格式: {"key1": "value1", "key2": "value2", ...}
        
        if (!file.exists()) {
            throw new RuntimeException("文件不存在: " + filePath);
        }
        
        JsonNode rootNode = objectMapper.readTree(file);
        
        if (rootNode.isArray()) {
            // 数组格式
            return objectMapper.convertValue(rootNode, new TypeReference<List<RedisKeyData>>() {});
        } else {
            // 对象格式，所有key都当作string类型处理
            List<RedisKeyData> result = new ArrayList<>();
            Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();
            
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                RedisKeyData keyData = new RedisKeyData();
                keyData.setKey(field.getKey());
                keyData.setType("string");
                keyData.setValue(field.getValue().asText());
                keyData.setTtl(-1); // 默认永不过期
                result.add(keyData);
            }
            
            return result;
        }
    }
    
    /**
     * 批量导入数据
     */
    private ImportResult batchImportData(List<RedisKeyData> keyDataList, int batchSize) {
        ImportResult result = new ImportResult();
        
        try (Jedis jedis = jedisPool.getResource()) {
            for (int i = 0; i < keyDataList.size(); i += batchSize) {
                int end = Math.min(i + batchSize, keyDataList.size());
                List<RedisKeyData> batch = keyDataList.subList(i, end);
                
                // 使用Pipeline批量处理
                Pipeline pipeline = jedis.pipelined();
                Map<RedisKeyData, Exception> batchErrors = processBatch(pipeline, batch);
                
                // 同步执行
                pipeline.sync();
                
                // 更新结果
                result.incrementSuccess(batch.size() - batchErrors.size());
                result.incrementFailed(batchErrors.size());
                result.addAllErrors(batchErrors);
                
                System.out.printf("进度: %d/%d (成功: %d, 失败: %d)%n",
                    end, keyDataList.size(), result.getSuccessCount(), result.getFailedCount());
                
                // 小延迟，避免对Redis造成太大压力
                if (i + batchSize < keyDataList.size()) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        
        System.out.println("导入完成！成功: " + result.getSuccessCount() + 
                          ", 失败: " + result.getFailedCount());
        
        if (!result.getErrors().isEmpty()) {
            System.out.println("错误详情:");
            for (Map.Entry<RedisKeyData, Exception> error : result.getErrors().entrySet()) {
                System.out.println("Key: " + error.getKey().getKey() + 
                                 ", 错误: " + error.getValue().getMessage());
            }
        }
        
        return result;
    }
    
    /**
     * 处理批次数据
     */
    private Map<RedisKeyData, Exception> processBatch(Pipeline pipeline, List<RedisKeyData> batch) {
        Map<RedisKeyData, Exception> errors = new HashMap<>();
        
        for (RedisKeyData keyData : batch) {
            try {
                // 先删除已存在的key（可选，根据需求决定）
                // pipeline.del(keyData.getKey());
                
                // 根据数据类型处理
                switch (keyData.getType().toLowerCase()) {
                    case "string":
                        handleStringType(pipeline, keyData);
                        break;
                    case "hash":
                        handleHashType(pipeline, keyData);
                        break;
                    case "list":
                        handleListType(pipeline, keyData);
                        break;
                    case "set":
                        handleSetType(pipeline, keyData);
                        break;
                    case "zset":
                        handleZSetType(pipeline, keyData);
                        break;
                    default:
                        throw new RuntimeException("不支持的数据类型: " + keyData.getType());
                }
                
                // 设置TTL
                if (keyData.getTtl() > 0) {
                    pipeline.expire(keyData.getKey(), keyData.getTtl());
                }
                
            } catch (Exception e) {
                errors.put(keyData, e);
            }
        }
        
        return errors;
    }
    
    private void handleStringType(Pipeline pipeline, RedisKeyData keyData) {
        pipeline.set(keyData.getKey(), keyData.getValue().toString());
    }
    
    @SuppressWarnings("unchecked")
    private void handleHashType(Pipeline pipeline, RedisKeyData keyData) {
        Map<String, String> hashData = (Map<String, String>) keyData.getValue();
        pipeline.hset(keyData.getKey(), hashData);
    }
    
    @SuppressWarnings("unchecked")
    private void handleListType(Pipeline pipeline, RedisKeyData keyData) {
        List<String> listData = (List<String>) keyData.getValue();
        if (!listData.isEmpty()) {
            pipeline.rpush(keyData.getKey(), listData.toArray(new String[0]));
        }
    }
    
    @SuppressWarnings("unchecked")
    private void handleSetType(Pipeline pipeline, RedisKeyData keyData) {
        Set<String> setData = (Set<String>) keyData.getValue();
        if (!setData.isEmpty()) {
            pipeline.sadd(keyData.getKey(), setData.toArray(new String[0]));
        }
    }
    
    @SuppressWarnings("unchecked")
    private void handleZSetType(Pipeline pipeline, RedisKeyData keyData) {
        List<Map<String, Object>> zsetData = (List<Map<String, Object>>) keyData.getValue();
        Map<String, Double> scoreMembers = new HashMap<>();
        
        for (Map<String, Object> member : zsetData) {
            String value = (String) member.get("value");
            Double score = ((Number) member.get("score")).doubleValue();
            scoreMembers.put(value, score);
        }
        
        if (!scoreMembers.isEmpty()) {
            pipeline.zadd(keyData.getKey(), scoreMembers);
        }
    }
    
    /**
     * 高性能导入（多线程版本）
     */
    public ImportResult parallelImportFromJsonFile(String filePath, int threadCount, int batchSize) 
            throws Exception {
        
        List<RedisKeyData> keyDataList = readJsonFile(filePath);
        System.out.println("读取到 " + keyDataList.size() + " 条数据，使用 " + threadCount + " 个线程");
        
        // 将数据分成多个批次
        List<List<RedisKeyData>> batches = splitIntoBatches(keyDataList, 
            (int) Math.ceil((double) keyDataList.size() / threadCount));
        
        ImportResult finalResult = new ImportResult();
        List<Thread> threads = new ArrayList<>();
        
        // 创建并启动线程
        for (int i = 0; i < batches.size(); i++) {
            List<RedisKeyData> batch = batches.get(i);
            Thread thread = new Thread(new ImportTask(batch, batchSize, finalResult));
            threads.add(thread);
            thread.start();
        }
        
        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }
        
        return finalResult;
    }
    
    private List<List<RedisKeyData>> splitIntoBatches(List<RedisKeyData> data, int batchSize) {
        List<List<RedisKeyData>> batches = new ArrayList<>();
        for (int i = 0; i < data.size(); i += batchSize) {
            int end = Math.min(i + batchSize, data.size());
            batches.add(data.subList(i, end));
        }
        return batches;
    }
    
    private class ImportTask implements Runnable {
        private final List<RedisKeyData> data;
        private final int batchSize;
        private final ImportResult finalResult;
        
        public ImportTask(List<RedisKeyData> data, int batchSize, ImportResult finalResult) {
            this.data = data;
            this.batchSize = batchSize;
            this.finalResult = finalResult;
        }
        
        @Override
        public void run() {
            try {
                ImportResult threadResult = batchImportData(data, batchSize);
                synchronized (finalResult) {
                    finalResult.incrementSuccess(threadResult.getSuccessCount());
                    finalResult.incrementFailed(threadResult.getFailedCount());
                    finalResult.addAllErrors(threadResult.getErrors());
                }
            } catch (Exception e) {
                System.err.println("导入线程失败: " + e.getMessage());
            }
        }
    }
    
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }


    public static void main(String[] args) {
        RedisDataImporter importer = null;
        Scanner scanner = new Scanner(System.in);

        try {
            int threadCount = 4;
            int batchSize = 100;
            String inputPath = "C:\\Users\\ayinj\\gitrepo\\space\\future_tick_1.3.json";

            // 创建导入器
            importer = new RedisDataImporter("192.168.1.3", 6380, "");

            ImportResult result = importer.parallelImportFromJsonFile(inputPath, threadCount, batchSize);

            System.out.println(result);

        } catch (Exception e) {
            System.err.println("导入失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (importer != null) {
                importer.close();
            }
            scanner.close();
        }
    }
}