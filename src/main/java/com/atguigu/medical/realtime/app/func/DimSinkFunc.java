package com.atguigu.medical.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.medical.realtime.bean.TableProcess;
import com.atguigu.medical.realtime.common.MedicalCommon;
import com.atguigu.medical.realtime.util.HBaseUtil;
import com.atguigu.medical.realtime.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * @author yhm
 * @create 2023-07-19 15:17
 */
public class DimSinkFunc extends RichSinkFunction<Tuple3<String, JSONObject, TableProcess>> {
    private Connection hBaseConnection =null;
    private Jedis jedis;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hBaseConnection = HBaseUtil.getHBaseConnection();
        jedis = RedisUtil.getRedisClient();
    }

    @Override
    public void invoke(Tuple3<String, JSONObject, TableProcess> value, Context context) throws Exception {
        // maxwell生成主流数据的类型
        String type = value.f0;
        JSONObject data = value.f1;
        TableProcess tableProcess = value.f2;

        String sinkTable = tableProcess.getSinkTable();
        String sinkRowKeyName = tableProcess.getSinkRowKey();
        String sinkRowKey = data.getString(sinkRowKeyName);
        String sinkColumns = tableProcess.getSinkColumns();
        String sinkFamily = tableProcess.getSinkFamily();
        String[] columns = sinkColumns.split(",");
        String[] values = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            values[i]=data.getString(columns[i]);
        }

        if ("delete".equals(type)){
            // 删除维度表数据
            HBaseUtil.deleteRow(hBaseConnection, MedicalCommon.HBASE_NAMESPACE,sinkTable,sinkRowKey);
        }else {
            // 写入维度表数据
            HBaseUtil.putRow(hBaseConnection, MedicalCommon.HBASE_NAMESPACE,sinkTable,sinkRowKey,sinkFamily,columns,values);
        }

        // 如果修改或者删除维度数据  需要把redis中缓存的数据一并删除
        if ("delete".equals(type) || "update".equals(type)){
            jedis.del(sinkTable+ ":" + sinkRowKey);
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        HBaseUtil.closeHBaseConnection(hBaseConnection);
    }
}
