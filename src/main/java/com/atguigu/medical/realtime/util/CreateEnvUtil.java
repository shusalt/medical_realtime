package com.atguigu.medical.realtime.util;

import com.atguigu.medical.realtime.common.MedicalCommon;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;
import java.util.Properties;

/**
 * @author yhm
 * @create 2023-07-18 16:32
 */
public class CreateEnvUtil {
    public static StreamExecutionEnvironment getStreamEnv(Integer port,String appName){
        // TODO 1 初始化流式环境
        Configuration conf = new Configuration();
        // 设置web监控的端口号
        conf.setInteger(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // TODO 2 设置检查点和状态后端
//        // 2.1 启动检查点
        env.enableCheckpointing(10*1000L);
        // 2.2 设置相邻两个检查点的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10*1000L);
        // 2.3 设置取消job时检查点的清理模式
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 2.4 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 2.5 设置状态后端的存储路径
        env.getCheckpointConfig().setCheckpointStorage(MedicalCommon.HDFS_URI_PREFIX + appName );
        // 2.6 修改系统的用户名称  对应hdfs的用户名
        System.setProperty("HADOOP_USER_NAME",MedicalCommon.HADOOP_USER_NAME);

        return env;
    }


    public static MySqlSource<String> getMysqlSource(){
        // JsonDeserialization不会解析decimal类型数据
        HashMap<String, Object> hashMap = new HashMap<>();
        hashMap.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());

        // mysql8.0版本及以后远程连接的参数
        Properties prop = new Properties();
        prop.setProperty("allowPublicKeyRetrieval","true");

        return MySqlSource.<String>builder()
                .hostname(MedicalCommon.MYSQL_HOSTNAME)
                .port(MedicalCommon.MYSQL_PORT)
                .username(MedicalCommon.MYSQL_USERNAME)
                .password(MedicalCommon.MYSQL_PASSWD)
                .databaseList(MedicalCommon.MEDICAL_CONFIG_DATABASE)
                .tableList(MedicalCommon.MEDICAL_CONFIG_TABLE)
                .jdbcProperties(prop)
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new JsonDebeziumDeserializationSchema(false,hashMap))
                // 是否需要先读一份完整的数据
                .startupOptions(StartupOptions.initial())
                .build();
    }

    public static void createOdsTable(StreamTableEnvironment tableEnv,String topicName,String groupId){
        tableEnv.executeSql("CREATE TABLE " + MedicalCommon.KAFKA_ODS_TOPIC + " (\n" +
                " `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` map<string,string>,\n" +
                "  `old` map<string,string> \n" +
                ")" + KafkaUtil.getKafkaDDL(topicName,groupId));
    }
}
