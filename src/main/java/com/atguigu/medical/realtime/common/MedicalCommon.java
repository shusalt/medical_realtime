package com.atguigu.medical.realtime.common;

/**
 * @author yhm
 * @create 2023-07-18 16:29
 */
public class MedicalCommon {
    // Kafka ODS 层主题名称
    public static String KAFKA_ODS_TOPIC = "topic_db";

    // HBase 关联的 Zookeeper 服务配置项
    public static String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    // HBase 关联的 Zookeeper 服务端口配置项
    public static String HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    // HBase 关联的 Zookeeper 服务主机名
    public static String HBASE_ZOOKEEPER_QUORUM_HOST = "hadoop102,hadoop103,hadoop104";
    // HBase 关联的 Zookeeper 服务端口
    public static String HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT_VALUE = "2181";
    // HBASE Schema 名称
    public static String HBASE_NAMESPACE = "MEDICAL_REALTIME";


    // Flink-CDC 连接的 MySQL 主机名
    public static String MYSQL_HOSTNAME = "hadoop102";
    // Flink-CDC 连接的 MySQL 端口号
    public static Integer MYSQL_PORT = 3306;
    // Flink-CDC 连接的 MySQL 用户名
    public static String MYSQL_USERNAME = "root";
    // Flink-CDC 连接的 MySQL 密码
    public static String MYSQL_PASSWD = "000000";
    // MySQL 驱动
    public static String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    // MySQL URL
    public static String MYSQL_URL = "jdbc:mysql://" + MYSQL_HOSTNAME + ":" + MYSQL_PORT + "?useSSL=false&allowPublicKeyRetrieval=true";
    // 配置表所属数据库名
    public static String MEDICAL_CONFIG_DATABASE = "medical_config";
    // 配置表名
    public static String MEDICAL_CONFIG_TABLE = "medical_config.table_process";

    // Kafka URI
    public static String KAFKA_BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    // Kafka 事务超时时间
    public static String KAFKA_TRANSACTION_TIMEOUT = 15 * 60 * 1000 + "";

    // Redis 主机名
    public static String REDIS_HOST = "hadoop102";
    // Redis 端口号
    public static Integer REDIS_PORT = 6379;

    // HDFS URI
    public static String HDFS_URI_PREFIX = "hdfs://hadoop102:8020/medical_realtime/ck/";
    // 操作 HDFS 的用户名
    public static String HADOOP_USER_NAME = "atguigu";

    // Doris FE 节点 IP：端口 组合
    public static String DORIS_FE_NODES = "hadoop102:7030";

    // Doris 用户名
    public static String DORIS_USER_NAME = "root";

    // Doris 密码
    public static String DORIS_PASSWD = "aaaaaa";
}
