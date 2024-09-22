package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2023-07-19 17:16
 */
public class FlinkSQL {
    public static void main(String[] args) {
        // 1. 创建flink运行的流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建flinkSQL的table环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 使用flinkSQL读取数据
        // 使用建表语句和不同库中的数据进行对应
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` map<string,string>,\n" +
                "  `old` map<string,string> \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
                "  'properties.group.id' = 'flinkSQL',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        tableEnv.sqlQuery("select * from topic_db").execute().print();



    }
}
