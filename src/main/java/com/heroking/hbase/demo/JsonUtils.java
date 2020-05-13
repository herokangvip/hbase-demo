package com.heroking.hbase.demo;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class JsonUtils {
    private static ObjectMapper mapper = new ObjectMapper();

    public static <T> String obj2String(T obj) {
        if (obj == null) {
            return null;
        }
        try {
            return obj instanceof String ? (String) obj : mapper.writeValueAsString(obj);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        User user = new User();
        user.setUserName("xiaoming");
        user.setAge(33);
        String s = JsonUtils.obj2String(user);
        System.out.println(s);
    }
}
