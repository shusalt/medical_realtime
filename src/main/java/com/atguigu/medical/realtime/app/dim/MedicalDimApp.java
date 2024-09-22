package com.atguigu.medical.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.medical.realtime.app.func.DimBroadcastFunc;
import com.atguigu.medical.realtime.app.func.DimSinkFunc;
import com.atguigu.medical.realtime.bean.TableProcess;
import com.atguigu.medical.realtime.common.MedicalCommon;
import com.atguigu.medical.realtime.util.CreateEnvUtil;
import com.atguigu.medical.realtime.util.HBaseUtil;
import com.atguigu.medical.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @author yhm
 * @create 2023-07-18 16:14
 */
public class MedicalDimApp {
    public static void main(String[] args) throws Exception {
        // TODO 1 初始化流式环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8081, "MedicalDimApp");
        env.setParallelism(1);

        // TODO 2 从topic_db主题中读取主流数据
        String topicName = MedicalCommon.KAFKA_ODS_TOPIC;
        String groupId = "medical_dim_app";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaConsumer(topicName, groupId);
        SingleOutputStreamOperator<String> kafkaSourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source").uid("kafka_source");

//        kafkaSourceStream.print("kafkaSourceStream");

        // TODO 3 使用flinkCDC读取配置表数据 作为从流
        MySqlSource<String> mysqlSource = CreateEnvUtil.getMysqlSource();

        SingleOutputStreamOperator<String> configSource = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "config_source")
                .uid("config_source")
                .setParallelism(1);

        configSource.print("config_source");
        // TODO 4 在HBase中创建维度表
        SingleOutputStreamOperator<TableProcess> tableCreateStream = configSource.process(new ProcessFunction<String, TableProcess>() {

            private Connection connection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 创建连接
                super.open(parameters);
                connection = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void processElement(String jsonStr, Context ctx, Collector<TableProcess> out) throws Exception {
                // 使用ali的fastJson处理json字符串
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                // op类型  CRUD -> create read update delete
                String op = jsonObject.getString("op");
                // 可以对应小驼峰和下划线的名称
                TableProcess tableProcess = jsonObject.getObject("after", TableProcess.class);
                TableProcess before = jsonObject.getObject("before", TableProcess.class);
                if ("r".equals(op) || "c".equals(op)) {
                    // 创建表格
                    createTable(tableProcess);
                } else if ("d".equals(op)) {
                    // 删除表格
                    dropTable(before);
                } else {
                    // update
                    // 一般不要去修改table_process表格的配置
                    dropTable(before);
                    createTable(tableProcess);
                }

                // 往下游输出数据
                tableProcess.setOperateType(op);
                out.collect(tableProcess);
            }

            private void createTable(TableProcess tableProcess) {
                HBaseUtil.createTable(connection, MedicalCommon.HBASE_NAMESPACE, tableProcess.sinkTable, tableProcess.sinkFamily.split(","));
            }

            private void dropTable(TableProcess tableProcess) {
                HBaseUtil.dropTable(connection, MedicalCommon.HBASE_NAMESPACE, tableProcess.sinkTable);
            }

            @Override
            public void close() throws Exception {
                super.close();
                HBaseUtil.closeHBaseConnection(connection);
            }
        });

        tableCreateStream.print("tableCreateStream");

        // TODO 5 广播配置流合并主流和从流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("broadcast_state",String.class, TableProcess.class);
        BroadcastStream<TableProcess> broadcastStream = tableCreateStream.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<String, TableProcess> connectedStream = kafkaSourceStream.connect(broadcastStream);

        // TODO 6 处理合并流数据  过滤出维度表
        // 返回值类型 <type,data,tableProcess>
        SingleOutputStreamOperator<Tuple3<String, JSONObject, TableProcess>> processStream = connectedStream.process(new DimBroadcastFunc(mapStateDescriptor));


        // TODO 7 写出到HBase
        processStream.addSink(new DimSinkFunc());

        // TODO 8 执行任务
        env.execute();
    }
}
