package com.ityj.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import java.util.List;

public class RedisKeyCleaner {
    
    private Jedis jedis;
    
    public RedisKeyCleaner(String host, int port, String password) {
        this.jedis = new Jedis(host, port);
        if (password != null && !password.isEmpty()) {
            this.jedis.auth(password);
        }
    }
    
    /**
     * 安全删除stock开头的所有key（分批删除，避免阻塞）
     */
    public long deleteStockKeys(String pattern, int batchSize) {
        System.out.println("开始扫描并删除匹配模式: " + pattern + " 的key");
        
        long totalDeleted = 0;
        String cursor = "0";
        ScanParams scanParams = new ScanParams().match(pattern).count(100);
        
        do {
            // 扫描匹配的key
            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
            List<String> keys = scanResult.getResult();
            cursor = scanResult.getCursor();
            
            if (!keys.isEmpty()) {
                // 分批删除
                for (int i = 0; i < keys.size(); i += batchSize) {
                    int end = Math.min(i + batchSize, keys.size());
                    List<String> batchKeys = keys.subList(i, end);
                    
                    // 批量删除
                    Long deleted = jedis.del(batchKeys.toArray(new String[0]));
                    totalDeleted += deleted;
                    
                    System.out.println("已删除批次: " + batchKeys.size() + " 个key, 累计: " + totalDeleted);
                    
                    // 小延迟，避免对Redis造成太大压力
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            
        } while (!cursor.equals("0"));
        
        System.out.println("删除完成！总共删除: " + totalDeleted + " 个key");
        return totalDeleted;
    }
    
    /**
     * 先统计数量，再确认删除
     */
    public void safeDeleteWithConfirmation(String pattern) {
        // 先统计匹配的key数量
        long count = countKeys(pattern);
        System.out.println("找到 " + count + " 个匹配模式 '" + pattern + "' 的key");
        
        if (count == 0) {
            System.out.println("没有找到匹配的key，无需删除");
            return;
        }
        
        // 这里可以添加确认逻辑
        System.out.println("警告：即将删除 " + count + " 个key！");
        System.out.println("请在5秒内终止程序来取消删除操作...");
        
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            System.out.println("操作已取消");
            return;
        }
        
        // 执行删除
        deleteStockKeys(pattern, 100);
    }
    
    /**
     * 统计匹配的key数量
     */
    public long countKeys(String pattern) {
        long count = 0;
        String cursor = "0";
        ScanParams scanParams = new ScanParams().match(pattern).count(100);
        
        do {
            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
            count += scanResult.getResult().size();
            cursor = scanResult.getCursor();
        } while (!cursor.equals("0"));
        
        return count;
    }
    
    public static void main(String[] args) {
        RedisKeyCleaner cleaner = new RedisKeyCleaner("192.168.1.3", 6380, "");
        
        try {
            // 方法1：直接删除（生产环境谨慎使用）
            // cleaner.deleteStockKeys("stock:*", 100);
            
            // 方法2：安全删除（推荐）
            cleaner.safeDeleteWithConfirmation("future_tick_*");
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (cleaner.jedis != null) {
                cleaner.jedis.close();
            }
        }
    }
}