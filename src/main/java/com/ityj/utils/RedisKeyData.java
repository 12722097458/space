package com.ityj.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis键数据模型
 */
class RedisKeyData {
    private String key;
    private String type;
    private Object value;
    private long ttl = -1; // -1表示永不过期
    
    // getters and setters
    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }
    
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    
    public Object getValue() { return value; }
    public void setValue(Object value) { this.value = value; }
    
    public long getTtl() { return ttl; }
    public void setTtl(long ttl) { this.ttl = ttl; }
    
    @Override
    public String toString() {
        return String.format("RedisKeyData{key='%s', type='%s', ttl=%d}", key, type, ttl);
    }
}

/**
 * 导入结果统计
 */
class ImportResult {
    private int successCount = 0;
    private int failedCount = 0;
    private Map<RedisKeyData, Exception> errors = new HashMap<>();
    
    public void incrementSuccess(int count) { successCount += count; }
    public void incrementFailed(int count) { failedCount += count; }
    
    public void addError(RedisKeyData keyData, Exception error) {
        errors.put(keyData, error);
    }
    
    public void addAllErrors(Map<RedisKeyData, Exception> newErrors) {
        errors.putAll(newErrors);
    }
    
    // getters
    public int getSuccessCount() { return successCount; }
    public int getFailedCount() { return failedCount; }
    public Map<RedisKeyData, Exception> getErrors() { return errors; }
    
    @Override
    public String toString() {
        return String.format("导入结果: 成功=%d, 失败=%d", successCount, failedCount);
    }
}