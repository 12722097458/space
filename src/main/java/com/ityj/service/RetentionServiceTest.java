package com.ityj.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
@SpringBootTest
public class RetentionServiceTest {

    @Autowired
    private RetentionService retentionService;

    @Test
    public void testSync() {
        System.out.println("开始执行飞书同步...");
        retentionService.sync();
        System.out.println("执行完成！");
    }
}