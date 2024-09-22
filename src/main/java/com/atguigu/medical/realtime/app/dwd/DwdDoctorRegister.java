package com.atguigu.medical.realtime.app.dwd;

import com.atguigu.medical.realtime.common.MedicalCommon;
import com.atguigu.medical.realtime.util.CreateEnvUtil;
import com.atguigu.medical.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2023-07-22 11:08
 */
public class DwdDoctorRegister {
    public static void main(String[] args) {
        // TODO 1 创建table环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8087, "medical_dwd_doctor_register");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // TODO 2 读取ods层数据
        String topicName = MedicalCommon.KAFKA_ODS_TOPIC;
        CreateEnvUtil.createOdsTable(tableEnv,topicName,"medical_dwd_doctor_register");

        // TODO 3 筛选数据
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] `id`, \n" +
                "    `data`['create_time'] `add_time`, \n" +
                "    `data`['birthday'] `birthday`, \n" +
                "    `data`['consultation_fee'] `consultation_fee`, \n" +
                "    `data`['gender'] `gender`, \n" +
                "    `data`['name'] `name`, \n" +
                "    `data`['specialty'] `specialty`, \n" +
                "    `data`['title'] `specialty`, \n" +
                "    `data`['hospital_id'] `user_id`\n" +
                "from topic_db\n" +
                "where `table`='doctor'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("result_table",resultTable);

        // TODO 4 写入到kafka对应主题
        String sinkTopic="medical_dwd_doctor_register";
        tableEnv.executeSql("create table " + sinkTopic + "(" +
                "  id string,\n" +
                "  create_time string,\n" +
                "  birthday string,\n" +
                "  consultation_fee string,\n" +
                "  gender string,\n" +
                "  name string,\n" +
                "  specialty string,\n" +
                "  title string,\n" +
                "  hospital_id string" +
                ")" + KafkaUtil.getKafkaSinkDDL(sinkTopic));

        tableEnv.executeSql("insert into " + sinkTopic + " select * from result_table");
    }
}
