package com.atguigu.medical.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.medical.realtime.bean.TableProcess;
import com.atguigu.medical.realtime.util.MySQLUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yhm
 * @create 2023-07-19 15:36
 */
public class DimBroadcastFunc extends BroadcastProcessFunction<String, TableProcess, Tuple3<String, JSONObject, TableProcess>> {
    // 如果主流数据先过来  没有配置表数据  会造成主流数据丢失
    private Map<String, TableProcess> configMap = new HashMap<String, TableProcess>();
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor = null;

    public DimBroadcastFunc(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    /**
     * 在处理主流数据之前 先判断mysql配置表中有那些表
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        java.sql.Connection connection = MySQLUtil.getConnection();
        PreparedStatement ps = connection.prepareStatement("select * from medical_config.table_process");
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            TableProcess tableProcess = new TableProcess();
            tableProcess.setSourceTable(resultSet.getString(1));
            tableProcess.setSinkTable(resultSet.getString(2));
            tableProcess.setSinkFamily(resultSet.getString(3));
            tableProcess.setSinkColumns(resultSet.getString(4));
            tableProcess.setSinkRowKey(resultSet.getString(5));
            configMap.put(resultSet.getString(1), tableProcess);
        }
        ps.close();
        connection.close();
    }

    @Override
    public void processElement(String jsonStr, ReadOnlyContext ctx, Collector<Tuple3<String, JSONObject, TableProcess>> out) throws Exception {
        // 通过configMap判断当前数据是否为维度表数据
        // maxwell生成的json数据
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        String type = jsonObject.getString("type");
        // 过滤掉data为空的数据
        if ("bootstrap-start".equals(type) || "bootstrap-complete".equals(type)) {
            return;
        }
        JSONObject data = jsonObject.getJSONObject("data");
        String table = jsonObject.getString("table");
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(table);

        // 判断tableProcess是否为空
        if (tableProcess == null) {
            // 广播状态中为空  则再去到初始化的map中获取
            tableProcess = configMap.get(table);
            if (tableProcess == null) {
                return;
            }
        }

        String[] columns = tableProcess.sinkColumns.split(",");
        // 判断当前的数据类型是否为删除
        if ("delete".equals(type)) {
            data = jsonObject.getJSONObject("old");
        } else {
            // 只保留下游需要写出的字段  不写出的字段删除
            data.keySet().removeIf(key -> !Arrays.asList(columns).contains(key));
        }
        // 按照设计好的返回值类型返回数据
        out.collect(Tuple3.of(type, data, tableProcess));

    }

    @Override
    public void processBroadcastElement(TableProcess tableProcess, Context ctx, Collector<Tuple3<String, JSONObject, TableProcess>> out) throws Exception {
        // 将配置流中的数据写入到广播状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 广播流过来的配置数据不一定是新增 也可能是删除
        String op = tableProcess.operateType;
        if ("d".equals(op)) {
            broadcastState.remove(tableProcess.sourceTable);
            configMap.remove(tableProcess.sourceTable);
        } else {
            broadcastState.put(tableProcess.sourceTable, tableProcess);
        }

    }
}
