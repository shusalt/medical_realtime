package com.atguigu.medical.realtime.app.dwd;

import com.atguigu.medical.realtime.common.MedicalCommon;
import com.atguigu.medical.realtime.util.CreateEnvUtil;
import com.atguigu.medical.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2023-07-22 10:45
 */
public class DwdUserRegister {
    public static void main(String[] args) {
        // TODO 1 创建table环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8086, "medical_dwd_user_register");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // TODO 2 读取ods层数据
        String topicName = MedicalCommon.KAFKA_ODS_TOPIC;
        CreateEnvUtil.createOdsTable(tableEnv,topicName,"medical_dwd_user_register");

        // TODO 3 筛选注册数据
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] `id`, \n" +
                "    `data`['create_time'] `register_time`, \n" +
                "    concat('*@',split_index(`data`['email'],'@',1)) `email`, \n" +
                "    concat(substr(`data`['telephone'],1,3),'********') `telephone`, \n" +
                "    `data`['username'] `username`\n" +
                "from topic_db\n" +
                "where `table`='user'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("result_table",resultTable);

        // TODO 4 写出到kafka对应主题
        String sinkTopic="medical_dwd_user_register";
        tableEnv.executeSql("create table " + sinkTopic + "(" +
                "id string,\n" +
                "register_time string,\n" +
                "email string,\n" +
                "telephone string,\n" +
                "username string " +
                ")" + KafkaUtil.getKafkaSinkDDL(sinkTopic));

        tableEnv.executeSql("insert into " + sinkTopic + " select * from result_table");
    }
}
