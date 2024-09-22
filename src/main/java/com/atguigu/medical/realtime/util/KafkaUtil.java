package com.atguigu.medical.realtime.util;

import com.atguigu.medical.realtime.common.MedicalCommon;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author yhm
 * @create 2023-07-18 17:45
 */
public class KafkaUtil {
    /**
     * 指定kafka主题和消费者组id获取kafka消费者对象
     * @param topicName 主题名称
     * @param groupId 消费者组id
     * @return
     */
    public static KafkaSource<String> getKafkaConsumer(String topicName,String groupId){
         return KafkaSource.<String>builder()
                .setBootstrapServers(MedicalCommon.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(topicName)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                // 此处不能使用SimpleStringSchema (默认value不能为null)
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message!=null && message.length!=0 ){
                            return new String(message, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .build();
    }

    public static String getKafkaDDL(String topicName,String groupId){
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                        "  'topic' = '"+ topicName + "',\n" +
                        "  'properties.bootstrap.servers' = '" + MedicalCommon.KAFKA_BOOTSTRAP_SERVERS + "',\n" +
                        "  'properties.group.id' = '" + groupId + "',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")";
    }

    public static String getKafkaSinkDDL(String topicName){
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+ topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + MedicalCommon.KAFKA_BOOTSTRAP_SERVERS + "',\n" +
                "  'format' = 'json'\n" +
                ")";
    }
}
