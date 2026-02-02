package com.ityj.utils;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class JsonReadUtil {
    public static void main(String[] args) {


        JSONObject days7Json = JSONUtil.readJSONObject(new File("C:\\Users\\ayinj\\gitrepo\\space\\src\\main\\resources\\inactive\\days7"), Charset.defaultCharset());
        JSONObject data = (JSONObject) days7Json.get("data");
        JSONArray phones = (JSONArray) data.get("phones");
        List<String> days7List = new ArrayList<>();
        for (Object phone : phones) {
            days7List.add((String) phone);
        }

        JSONObject days8Json = JSONUtil.readJSONObject(new File("C:\\Users\\ayinj\\gitrepo\\space\\src\\main\\resources\\inactive\\days8"), Charset.defaultCharset());
        JSONObject data2 = (JSONObject) days8Json.get("data");
        JSONArray phones2 = (JSONArray) data2.get("phones");
        List<String> days8List = new ArrayList<>();
        for (Object phone : phones2) {
            days8List.add((String) phone);
        }

        List<String> inactiveList = new ArrayList<>();
        for (String phone : days8List) {
            if (!days7List.contains(phone)) {
                inactiveList.add(phone);
            }
        }

        System.out.println(inactiveList);
        System.out.println(inactiveList.size());

    }
}
