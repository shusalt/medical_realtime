package com.atguigu.medical.realtime.app.dwd;

import com.atguigu.medical.realtime.common.MedicalCommon;
import com.atguigu.medical.realtime.util.CreateEnvUtil;
import com.atguigu.medical.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2023-07-20 17:17
 */
public class DwdTradePrescriptionPaySuc {
    public static void main(String[] args) {
        // TODO 1 创建表格环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8085, "medical_dwd_trade_prescription_pay_suc");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // TODO 2 读取ods层数据
        // 2.1 处方详情表注入水位线作为事件时间
        String topicName = MedicalCommon.KAFKA_ODS_TOPIC;
        String groupId = "medical_dwd_trade_prescription_pay_suc";
        tableEnv.executeSql("CREATE TABLE " + MedicalCommon.KAFKA_ODS_TOPIC  + "_detail" + " (\n" +
                " `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` map<string,string>,\n" +
                "  `old` map<string,string>, \n" +
                "  `row_time` as TO_TIMESTAMP(`data`['create_time']) , \n" +
                "  WATERMARK FOR `row_time`  AS `row_time` - INTERVAL '5' SECOND \n" +
                ")"  + KafkaUtil.getKafkaDDL(topicName, groupId + "_detail"));

        // 2.2 支付成功表注入水位线作为事件时间
        tableEnv.executeSql("CREATE TABLE " + MedicalCommon.KAFKA_ODS_TOPIC  + "_suc" + " (\n" +
                " `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` map<string,string>,\n" +
                "  `old` map<string,string>, \n" +
                "  `row_time` as TO_TIMESTAMP(`data`['update_time']) , \n" +
                "  WATERMARK FOR `row_time`  AS `row_time` - INTERVAL '5' SECOND \n" +
                ")"  + KafkaUtil.getKafkaDDL(topicName, groupId + "_suc"));

        // TODO 3 筛选数据
        // 3.1 筛选处方详情表数据
        Table prescriptionDetail = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id, \n" +
                "    `data`['create_time'] create_time, \n" +
                "    `data`['count'] `count`, \n" +
                "    `data`['medicine_id'] medicine_id, \n" +
                "    `data`['prescription_id'] prescription_id, \n" +
                "    `row_time` \n" +
                "from topic_db" + "_detail" + "\n" +
                "where `table`='prescription_detail'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("prescription_detail",prescriptionDetail);

        // 3.2 筛选支付成功表数据
        Table prescriptionPaySuc = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id, \n" +
                "    `data`['update_time'] update_time, \n" +
                "    `data`['total_amount'] total_amount, \n" +
                "    `data`['consultation_id'] consultation_id, \n" +
                "    `data`['doctor_id'] doctor_id, \n" +
                "    `data`['patient_id'] patient_id ,\n" +
                "    `row_time` \n" +
                "from topic_db" + "_suc" + "\n" +
                "where `table`='prescription'\n" +
                "and `type`='update'" +
                "and `data`['status']='203'" +
                "");
        tableEnv.createTemporaryView("prescription_pay_suc",prescriptionPaySuc);

        // TODO 4 使用interval join 连接数据
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "    `pd`.id ,\n" +
                "    `pps`.update_time prescription_pay_suc_time,\n" +
                "    `count`,\n" +
                "    medicine_id,\n" +
                "    prescription_id,\n" +
                "    total_amount,\n" +
                "    consultation_id,\n" +
                "    doctor_id,\n" +
                "    patient_id\n" +
                "from `prescription_detail` `pd` ,`prescription_pay_suc` `pps`\n" +
                "where `pd`.`prescription_id` =  `pps`.`id`\n" +
                "and `pd`.`row_time` >= `pps`.row_time - INTERVAL '15' minute\n" +
                "and `pd`.`row_time` <= `pps`.row_time + INTERVAL '5' second");
        tableEnv.createTemporaryView("result_table",resultTable);

        // TODO 5 写出到kafka对应主题
        String sinkTopic="medical_dwd_trade_prescription_pay_suc";
        tableEnv.executeSql("create table  " + sinkTopic + " (\n" +
                "    `id` string,\n" +
                "    `prescription_pay_suc_time` string,\n" +
                "    `count` string,\n" +
                "    `medicine_id` string,\n" +
                "    `prescription_id` string,\n" +
                "    `total_amount` string,\n" +
                "    `consultation_id` string,\n" +
                "    `doctor_id` string,\n" +
                "    `patient_id` string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL(sinkTopic));
        tableEnv.executeSql("insert into " + sinkTopic + " select * from result_table");

    }
}
