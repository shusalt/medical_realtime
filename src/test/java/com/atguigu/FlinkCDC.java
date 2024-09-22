package com.atguigu;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2023-07-18 15:49
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1. 创建flink运行的流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并发
        env.setParallelism(1);

        // 2. 使用flinkCDC实时监控获取medical_config中的表
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("medical_config")
                .tableList("medical_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 是否需要先读一份完整的数据
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> flinkCDC = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "flinkCDC");

        // 3. 打印flinkCDC监控的数据
        flinkCDC.print();

        // 4. 执行任务
        env.execute();
    }
}
