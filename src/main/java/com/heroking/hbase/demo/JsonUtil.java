package com.heroking.hbase.demo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * json转换方面的工具类,全部为静态方法
 *
 * @author xx
 */
public class JsonUtil {
    private static final Logger log = LoggerFactory.getLogger(JsonUtil.class);
    private static ObjectMapper mapper = new ObjectMapper();

    static {
        // 设置序列化配置，为null的属性不加入到json中
        mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false); // map对象序列化的时候忽略值为null的key
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE).
                setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    private JsonUtil() {

    }

    /**
     * 将对象转换成json字符串,如果转换失败则返回null
     *
     * @param o 需要转换为json的对象
     * @return 转换后的json字符串
     */
    public static String obj2JsonStr(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            log.error("write2JsonStr, JsonUtil exception", e);
        }
        return "";
    }

    /**
     * 将json转换为对象 如果对象模版为内部类会出现问题，所以不要使用内部类
     *
     * @param json  要转换的json
     * @param clazz 要映射的类型
     * @return 转换成的目标对象，如果转换失败返回null
     */
    public static Object jsonStr2Obj(String json, Class<?> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (Exception e) {
            log.error("json2Object, JsonUtil exception, json is:" + json, e);
        }
        return null;
    }

    /**
     * 将json字符串转换为Map
     *
     * @param json 需要转换为Map的json字符串 {}开头结尾的
     * @return 转换后的map 如果转换失败返回null
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> json2Map(String json) {
        try {
            if (json == null || "".equals(json.trim())) {
                return new HashMap<String, Object>();
            }
            return mapper.readValue(json, Map.class);
        } catch (Exception e) {
            log.error("json2Map, JsonUtil exception, json is:" + json, e);
        }
        return new HashMap<String, Object>();
    }

    /**
     * 将json数组转换为List<Map<String,Object>> json数组格式[{},{}]
     *
     * @param jsonArray 需要转换的json数组
     * @return 转换后的列表 如果转换失败返回null
     */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> jsonArray2List(String jsonArray) {
        try {
            return mapper.readValue(jsonArray, List.class);
        } catch (Exception e) {
            log.error("jsonArray2List, JsonUtil exception, json is:" + jsonArray, e);
        }
        return new ArrayList<Map<String, Object>>();
    }

    /**
     * json array 转换成list对象
     *
     * @param json
     * @param tr   new TypeReference<List<User>>() {}
     * @return
     */
    public static List<?> json2ArrayObject(String json, TypeReference<?> tr) {
        try {
            return mapper.readValue(json, tr);
        } catch (Exception e) {
            log.error("json2ArrayObject, JsonUtil exception, json is:" + json, e);
        }
        return null;
    }

    /**
     * <String, User>  这种类型的hashmap 转换
     *
     * @param json
     * @param keyClazz
     * @param valueClazz
     * @return
     */
    public static Map<?, ?> json2MapObject(String json, Class<?> keyClazz, Class<?> valueClazz) {
        try {
            JavaType javaType = mapper.getTypeFactory().constructParametricType(HashMap.class, keyClazz, valueClazz);
            return mapper.readValue(json, javaType);
        } catch (Exception e) {
            log.error("json2MapObject, JsonUtil exception, json is:" + json, e);
        }
        return null;
    }

    public static JsonNode json2Node(String json) {
        try {
            return mapper.readValue(json, JsonNode.class);
        } catch (Exception e) {
            log.error("json2Node, JsonUtil exception, json is:" + json, e);
        }

        return null;
    }

    public static void node2Writer(Writer writer, JsonNode node) {
        try {
            mapper.writeValue(writer, node);
        } catch (Exception e) {
            log.error("node2Writer, JsonUtil exception, json is:" + node, e);
        }
    }

}
