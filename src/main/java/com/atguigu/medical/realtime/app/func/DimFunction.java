package com.atguigu.medical.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author yhm
 * @create 2023-07-22 15:39
 */
public interface DimFunction<T> {
    String getTable();
    String getId( T bean);
    void addDim(T bean, JSONObject dim);
}
