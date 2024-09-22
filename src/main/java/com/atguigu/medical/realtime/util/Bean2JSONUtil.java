package com.atguigu.medical.realtime.util;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.medical.realtime.bean.TransientSink;

import java.lang.reflect.Field;

/**
 * @author yhm
 * @create 2023-07-23 10:27
 */
public class Bean2JSONUtil {

    public static <T> String bean2JSON(T obj) throws IllegalAccessException {
        // 获取泛型的类模板
        Class<?> clazz = obj.getClass();
        Field[] fields = clazz.getDeclaredFields();

        JSONObject jsonObject = new JSONObject();
        for (Field field : fields) {
            // 首先判断当前字段是否有注解
            TransientSink annotation = field.getAnnotation(TransientSink.class);
            if (annotation == null){
                // 当前字段需要写入json中
                field.setAccessible(true);
                String name = field.getName();
                Object value = field.get(obj);
                // 将格式转换为蛇形
                StringBuilder snakeCaseName = new StringBuilder();
                for (int i = 0; i < name.length(); i++) {
                    if (Character.isUpperCase(name.charAt(i))){
                        // 当前字符为大写
                        snakeCaseName.append("_").append(Character.toLowerCase(name.charAt(i)));
                    }else {
                        // 当前字符为小写
                        snakeCaseName.append(name.charAt(i));
                    }
                }
                // 转换为蛇形之后写入到jsonObj
                jsonObject.put(snakeCaseName.toString(),value);
            }else {
                // 当前字段不需要写入json中
            }
        }

        return jsonObject.toJSONString();
    }
}
