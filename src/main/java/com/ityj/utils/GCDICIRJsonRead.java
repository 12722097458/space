package com.ityj.utils;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.*;

public class GCDICIRJsonRead {


    //RETURN：691c4518f32c48f0aa1380ab
    //IR: 691cd606dcd5b91edced32f5
    //IC： 68c390e0865aed3850c4475a

    private static final String PATH = "C:\\Users\\ayinj\\Desktop\\panda.factor_competition_indicator69036bc24f0693a4fdef7b0b.json";
    @Test // 当日持仓
    public void testIR_1() {
        JSONArray jsonArr = JSONUtil.readJSONArray(new File(PATH), Charset.defaultCharset());
        JSONObject json = (JSONObject) jsonArr.get(0);
        JSONArray data = (JSONArray) json.get("current_position_df");
        List<Map<String, Object>> res = new ArrayList<>();
        for (Object datum : data) {
            JSONObject jsonObject = (JSONObject) datum;
            String symbol = jsonObject.get("symbol").toString();
            String name = jsonObject.get("name").toString();
            BigDecimal factorValue = new BigDecimal(String.valueOf(jsonObject.get("factor_value")));

            HashMap<String, Object> objectObjectHashMap = new HashMap<>();
            objectObjectHashMap.put("symbol", symbol);
            objectObjectHashMap.put("name", name);
            objectObjectHashMap.put("factor_value", factorValue);
            res.add(objectObjectHashMap);
        }
        // 按照factorValue从大到小排序

        res.sort((o1, o2) -> {
            BigDecimal v2 = (BigDecimal) o2.get("factor_value");
            BigDecimal v1 = (BigDecimal) o1.get("factor_value");
            return v2.compareTo(v1); // 降序排列
        });
        System.out.println("res = " + res);
    }


    @Test    // 当日收益
    public void testIR_2() {
        JSONArray jsonArr = JSONUtil.readJSONArray(new File(PATH), Charset.defaultCharset());
        JSONObject json = (JSONObject) jsonArr.get(0);
        JSONArray data = (JSONArray) json.get("day_return_df");
        List<Map<String, Object>> res = new ArrayList<>();
        for (Object datum : data) {
            JSONObject jsonObject = (JSONObject) datum;
            String symbol = jsonObject.get("symbol").toString();
            String name = jsonObject.get("name").toString();
            String positionFlag = jsonObject.get("position_flag").toString();
            BigDecimal preReturn = new BigDecimal(String.valueOf(jsonObject.get("pre_return")));
            BigDecimal curReturn = new BigDecimal(String.valueOf(jsonObject.get("cur_return")));

            HashMap<String, Object> objectObjectHashMap = new HashMap<>();
            objectObjectHashMap.put("symbol", symbol);
            objectObjectHashMap.put("name", name);
            objectObjectHashMap.put("position_flag", positionFlag);
            objectObjectHashMap.put("pre_return", preReturn);
            objectObjectHashMap.put("cur_return", curReturn);
            res.add(objectObjectHashMap);
        }
        // 按照cur_return从大到小排序

        res.sort((o1, o2) -> {
            BigDecimal v2 = (BigDecimal) o2.get("cur_return");
            BigDecimal v1 = (BigDecimal) o1.get("cur_return");
            return v2.compareTo(v1); // 降序排列
        });
        System.out.println("res = " + res);
    }
}
