package com.ityj.utils;

import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class ResolveIpTest {

    private  static  String updateScript = "update event_registration set region_name = '%s' where event_id = 3 and user_id = %s;";

    static RestTemplate restTemplate = new RestTemplate();

    public static void main(String[] args) {
        // 1. 读取excel C:\Users\ayinj\Desktop\k8s-log-ca29bbf76013145ae80715e56ce1f199c_event-log_20260114_123526.xlsx, 第一列是user_id, 第二列是ip
        // 2. ip通过getRegionByIp()转换成省份
        // 3. 更新数据库 event_registration 表的 region_name 字段， 输出updateScript

        // Excel文件路径
        // Excel文件路径
//        String excelFilePath = "C:\\Users\\ayinj\\Desktop\\k8s-log-ca29bbf76013145ae80715e56ce1f199c_event-log_20260114_123526.xlsx";
        // src/main/resources/发布会信息表更新
        String excelFilePath = "C:\\Users\\ayinj\\Desktop\\k8s-log-ca29bbf76013145ae80715e56ce1f199c_event-log_20260114_130644.xlsx";
        String outputFilePath = "output.sql"; // 输出SQL文件路径

        try (FileWriter writer = new FileWriter(outputFilePath)) {
            // 读取Excel文件
            ExcelReader reader = ExcelUtil.getReader(excelFilePath);
            List<Map<String, Object>> readAll = reader.readAll();

            System.out.println("共读取到 " + readAll.size() + " 条记录");

            // 遍历每条记录
            for (Map<String, Object> row : readAll) {
                // 第一列(user_id)索引为0，第二列(ip)索引为1
                String userId = String.valueOf(row.get("user_id")); // 第一列是用户ID
                String ip = String.valueOf(row.get("ip"));      // 第二列是IP地址

                // 跳过可能的标题行或空值
                if ("user_id".equals(userId.toLowerCase()) || "userid".equals(userId.toLowerCase())) {
                    continue; // 跳过标题行
                }

                if (userId == null || userId.trim().isEmpty() || "null".equals(userId)) {
                    continue; // 跳过空用户ID
                }

                if (ip == null || ip.trim().isEmpty() || "null".equals(ip)) {
                    continue; // 跳过空IP
                }

                // 通过IP获取地区信息
                String regionName = getRegionByIp(ip.trim());

                // 生成SQL语句
                String sql = String.format(updateScript,
                        regionName.replace("'", "''"), // 防止SQL注入，转义单引号
                        userId.replace("'", "''")); // 使用user_id作为匹配条件，防止SQL注入，转义单引号

                // 写入SQL到文件
                writer.write(sql + "\n");

            }

            System.out.println("SQL语句已写入到 " + outputFilePath);
        } catch (IOException e) {
            log.error("写入文件时出错", e);
            System.out.println("写入文件时出错: " + e.getMessage());
        } catch (Exception e) {
            log.error("处理Excel文件时出错", e);
            System.out.println("处理Excel文件时出错: " + e.getMessage());
        }
    }



    private static final String IP_API_URL = "http://ip-api.com/json/{ip}?lang=zh-CN";

    public static String getRegionByIp(String ip) {
        try {
            ResponseEntity<IpApiResponse> response = restTemplate.getForEntity(
                    IP_API_URL, IpApiResponse.class, ip);

            IpApiResponse body = response.getBody();
            if (body == null || !"success".equalsIgnoreCase(body.getStatus())) {
                return "";
            }

            // 省份
            return Optional.ofNullable(body.getRegionName()).orElse("");
        } catch (Exception e) {
            // 失败时避免影响登录流程
            return "";
        }
    }

    @Data
    public static class IpApiResponse {
        private String status;
        private String country;
        private String countryCode;
        private String region;
        private String regionName;
        private String city;
        private String isp;
        private String query;
    }


}
