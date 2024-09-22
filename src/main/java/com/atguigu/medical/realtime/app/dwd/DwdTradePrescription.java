package com.atguigu.medical.realtime.app.dwd;

import com.atguigu.medical.realtime.common.MedicalCommon;
import com.atguigu.medical.realtime.util.CreateEnvUtil;
import com.atguigu.medical.realtime.util.KafkaUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2023-07-20 15:29
 */
public class DwdTradePrescription {
    public static void main(String[] args) {
        // TODO 1 创建table环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8084, "medical_dwd_trade_prescription");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // TODO 2 读取ods层数据
        String topicName = MedicalCommon.KAFKA_ODS_TOPIC;
        CreateEnvUtil.createOdsTable(tableEnv,topicName,"medical_dwd_trade_prescription");

        // flinkSQL在进行join的时候必须要有优化的手段
        // 默认join是将所有的数据永久保存到内存当中
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl","5 s");

        // TODO 3 筛选处方表
        Table preTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id, \n" +
                "    `data`['create_time'] create_time, \n" +
                "    `data`['total_amount'] total_amount, \n" +
                "    `data`['consultation_id'] consultation_id, \n" +
                "    `data`['doctor_id'] doctor_id, \n" +
                "    `data`['patient_id'] patient_id\n" +
                "from topic_db\n" +
                "where `table`='prescription'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("prescription",preTable);

        // TODO 4 筛选处方详情表
        Table preDetailTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id, \n" +
                "    `data`['create_time'] create_time, \n" +
                "    `data`['count'] `count`, \n" +
                "    `data`['medicine_id'] medicine_id, \n" +
                "    `data`['prescription_id'] prescription_id\n" +
                "from topic_db\n" +
                "where `table`='prescription_detail'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("prescription_detail",preDetailTable);

        // TODO 5 合并两张表格
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "    det.id ,\n" +
                "    det.create_time prescription_time,\n" +
                "    `count`,\n" +
                "    medicine_id,\n" +
                "    prescription_id,\n" +
                "    total_amount,\n" +
                "    consultation_id,\n" +
                "    doctor_id,\n" +
                "    patient_id\n" +
                "from prescription pre\n" +
                "join prescription_detail det\n" +
                "on pre.id=det.prescription_id");
        tableEnv.createTemporaryView("result_table",resultTable);


        // TODO 6 写出到kafka对应的主题
        String sinkTopic="medical_dwd_trade_prescription";
        tableEnv.executeSql("create table  " + sinkTopic + " (\n" +
                "    `id` string,\n" +
                "    `prescription_time` string,\n" +
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
