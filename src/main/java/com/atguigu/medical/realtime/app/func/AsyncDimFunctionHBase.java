package com.atguigu.medical.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.medical.realtime.common.MedicalCommon;
import com.atguigu.medical.realtime.util.HBaseUtil;
import com.atguigu.medical.realtime.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author yhm
 * @create 2023-07-22 14:30
 */
public abstract class AsyncDimFunctionHBase<T>  extends RichAsyncFunction<T,T> implements DimFunction<T> {

    private AsyncConnection hBaseAsyncConnection = null;
    private StatefulRedisConnection<String, String> asyncRedisConnection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseAsyncConnection = HBaseUtil.getHBaseAsyncConnection();
        asyncRedisConnection = RedisUtil.getAsyncRedisConnection();
    }


    @Override
    public void asyncInvoke(T bean, ResultFuture<T> resultFuture) throws Exception {

        CompletableFuture.supplyAsync(new Supplier<JSONObject>() {
            @Override
            public JSONObject get() {
                //1. 先从redis中异步读取维度信息
                // redis 的key是 table:id
                return RedisUtil.asyncReadDim(asyncRedisConnection,getTable() + ":" + getId(bean));
            }
        })
                .thenApplyAsync(new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dim) {
                        //2. 判断redis中是否存在对应的维度信息
                        if (dim == null){
                            System.out.println("从hbase中读取维度数据");
                            // 如果不存在 异步从hbase中读取redis信息
                            dim = HBaseUtil.asyncReadDim(hBaseAsyncConnection, MedicalCommon.HBASE_NAMESPACE,getTable(),getId(bean));
                            if (dim == null){
                                //5. 如果都没有维度信息  报错
                                System.out.println("没有能够匹配的维度信息");
                            }
                            //3. 将hbase中读取的维度信息写入到redis中
                            RedisUtil.asyncWriteDim(asyncRedisConnection,getTable() + ":" + getId(bean),dim);
                        }

                        return dim;
                    }
                }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject dim) {
                //4. 合并维度信息返回对应的数据
                addDim(bean,dim);
                // 输出合并之后的结果
                resultFuture.complete(Collections.singletonList(bean));
            }
        });
    }

    @Override
    public void close() throws Exception {
        super.close();
//        HBaseUtil.closeAsyncHBaseConnection(hBaseAsyncConnection);
//        asyncRedisConnection.close();
    }

}
