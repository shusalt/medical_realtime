package com.atguigu.medical.realtime.app.dwd;

import com.atguigu.medical.realtime.common.MedicalCommon;
import com.atguigu.medical.realtime.util.CreateEnvUtil;
import com.atguigu.medical.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2023-07-19 17:30
 */
public class DwdTradeConsultation {
    public static void main(String[] args) {
        // TODO 1 创建table环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8082, "dwd_trade_consultation");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // TODO 2 读取ods数据
        String groupId = "dwd_trade_consultation1";
        CreateEnvUtil.createOdsTable(tableEnv, MedicalCommon.KAFKA_ODS_TOPIC, groupId);

        // TODO 3 筛选数据
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id, \n" +
                "    `data`['create_time'] create_time, \n" +
                "    `data`['consultation_fee'] consultation_fee, \n" +
                "    `data`['doctor_id'] doctor_id, \n" +
                "    `data`['patient_id'] patient_id, \n" +
                "    `data`['user_id'] user_id\n" +
                "from topic_db\n" +
                "where `table`='consultation'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("result_table",resultTable);

        // TODO 4 写回到新的dwd层kafka主题中
        String sinkTopic = "medical_dwd_trade_consultation";
        tableEnv.executeSql("create table " + sinkTopic + "( \n" +
                "    `id` string,\n" +
                "    `create_time` string,\n" +
                "    `consultation_fee` string,\n" +
                "    `doctor_id` string,\n" +
                "    `patient_id` string,\n" +
                "    `user_id` string" +
                ")" + KafkaUtil.getKafkaSinkDDL(sinkTopic));

        tableEnv.executeSql("insert into " + sinkTopic + " select * from result_table");

    }

}
