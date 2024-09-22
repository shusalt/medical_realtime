package com.atguigu.medical.realtime.app.dwd;

import com.atguigu.medical.realtime.common.MedicalCommon;
import com.atguigu.medical.realtime.util.CreateEnvUtil;
import com.atguigu.medical.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2023-07-20 14:59
 */
public class DwdTradeConsultationPaySuc {
    public static void main(String[] args) {
        // TODO 1 创建table环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8083, "medical_dwd_trade_consultation_pay_suc");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // TODO 2 读取ods层数据
        String topicName = MedicalCommon.KAFKA_ODS_TOPIC;
        CreateEnvUtil.createOdsTable(tableEnv,topicName,"medical_dwd_trade_consultation_pay_suc");

        // TODO 3 筛选数据
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id, \n" +
                "    `data`['update_time'] consultation_pay_suc_time, \n" +
                "    `data`['consultation_fee'] consultation_fee, \n" +
                "    `data`['doctor_id'] doctor_id, \n" +
                "    `data`['patient_id'] patient_id, \n" +
                "    `data`['user_id'] user_id\n" +
                "from topic_db\n" +
                "where `table`='consultation'\n" +
                "and `type`='update'\n" +
                "and `data`['status']='203'");
        tableEnv.createTemporaryView("result_table",resultTable);


        // TODO 4 写出到对应的kafka主题
        String sinkTopic="medical_dwd_trade_consultation_pay_suc";
        tableEnv.executeSql("create table " + sinkTopic + "  (\n" +
                "    `id` string,\n" +
                "    `consultation_pay_suc_time` string,\n" +
                "    `consultation_fee` string,\n" +
                "    `doctor_id` string,\n" +
                "    `patient_id` string,\n" +
                "    `user_id` string\n" +
                ") " + KafkaUtil.getKafkaSinkDDL(sinkTopic));
        tableEnv.executeSql("insert into " + sinkTopic + " select * from result_table");


    }
}
