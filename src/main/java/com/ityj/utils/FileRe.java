package com.ityj.utils;

import cn.hutool.core.io.FileUtil;

import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class FileRe {


    public static void main(String[] args) {

        List<String> datesBetween = getDatesBetween("20251218", "20251218");
        System.out.println("datesBetween = " + datesBetween);

    }

    /**
     * 获取指定日期范围内的所有日期
     * @param startDate 开始日期，格式：yyyyMMdd
     * @param endDate 结束日期，格式：yyyyMMdd
     * @return 日期列表，格式为yyyyMMdd
     */
    private static List<String> getDatesBetween(String startDate, String endDate) {
        List<String> dates = new ArrayList<>();
        try {
            // 解析日期格式
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            LocalDate start = LocalDate.parse(startDate, formatter);
            LocalDate end = LocalDate.parse(endDate, formatter);

            // 生成日期范围内的所有日期
            LocalDate currentDate = start;
            while (!currentDate.isAfter(end)) {
                dates.add(currentDate.format(formatter));
                currentDate = currentDate.plusDays(1);
            }
        } catch (Exception e) {
        }
        return dates;
    }






}
