package com.atguigu.medical.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.medical.realtime.common.MedicalCommon;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * @author yhm
 * @create 2023-07-19 10:46
 */
public class HBaseUtil {
    /**
     * 创建hbase的同步连接
     * @return
     */
    public static Connection getHBaseConnection(){
        // 创建配置对象
        Configuration conf = HBaseConfiguration.create();
        // 添加配置参数
        conf.set(MedicalCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT,MedicalCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT_VALUE);
        conf.set(MedicalCommon.HBASE_ZOOKEEPER_QUORUM,MedicalCommon.HBASE_ZOOKEEPER_QUORUM_HOST);

        try {
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 关闭连接
     * @param conn
     */
    public static void closeHBaseConnection(Connection conn){
        if (conn != null && !conn.isClosed()){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 删除表格
     * @param connection 连接
     * @param hbaseNamespace 命名空间
     * @param tableName 表名
     */
    public static void dropTable(Connection connection, String hbaseNamespace, String tableName) {
        try {
            Admin admin = connection.getAdmin();
            TableName tableName1 = TableName.valueOf(hbaseNamespace,tableName);
            // 判断是否有这个表格
            if (admin.tableExists(tableName1)){
                admin.disableTable(tableName1);
                admin.deleteTable(tableName1);
            }else {
                System.out.println("要删除的表格"   + hbaseNamespace + ":" + tableName + "不存在");
            }
            admin.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * 创建表格
     * @param connection 连接
     * @param hbaseNamespace 命名空间
     * @param tableName 表名
     * @param families 列族名
     */
    public static void createTable(Connection connection, String hbaseNamespace, String tableName, String... families) {
        if (families == null ||  families.length < 1){
            throw new RuntimeException("至少有一个列族");
        }
        try {
            Admin admin = connection.getAdmin();
            TableName tableName1 = TableName.valueOf(hbaseNamespace, tableName);
            if (admin.tableExists(tableName1)){
                System.out.println("需要创建的表格"  + hbaseNamespace + ":" + tableName + "已经存在");
            }else{
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName1);
                for (String family : families) {
                    // 添加多个列族
                    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family));
                    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
                }
                admin.createTable(tableDescriptorBuilder.build());

            }
            admin.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void deleteRow(Connection hBaseConnection, String hbaseNamespace, String sinkTable, String sinkRowKey) {
        try {
            Table table = hBaseConnection.getTable(TableName.valueOf(hbaseNamespace, sinkTable));

            table.delete(new Delete(Bytes.toBytes(sinkRowKey)));

            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void putRow(Connection hBaseConnection, String hbaseNamespace, String sinkTable, String sinkRowKey, String sinkFamily, String[] columns, String[] values) {
        try {
            Table table = hBaseConnection.getTable(TableName.valueOf(hbaseNamespace, sinkTable));
            Put put = new Put(Bytes.toBytes(sinkRowKey));
            for (int i = 0; i < columns.length; i++) {
                // 有可能部分列值为空
                if (values[i] != null){
                    put.addColumn(Bytes.toBytes(sinkFamily),Bytes.toBytes(columns[i]),Bytes.toBytes(values[i]));
                }
            }
            table.put(put);
            table.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建异步连接
     * @return
     */
    public static AsyncConnection getHBaseAsyncConnection(){
        // 创建配置对象
        Configuration conf = HBaseConfiguration.create();
        // 添加配置参数
        conf.set(MedicalCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT,MedicalCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT_VALUE);
        conf.set(MedicalCommon.HBASE_ZOOKEEPER_QUORUM,MedicalCommon.HBASE_ZOOKEEPER_QUORUM_HOST);

        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 关闭异步连接
     * @param conn
     */
    public static void closeAsyncHBaseConnection(AsyncConnection conn){
        if (conn != null && !conn.isClosed()){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 从hbase中异步读取数据
     * @param hBaseAsyncConnection
     * @param hbaseNamespace
     * @param tableName
     * @param rowKey
     * @return
     */
    public static JSONObject asyncReadDim(AsyncConnection hBaseAsyncConnection, String hbaseNamespace, String tableName, String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> table = hBaseAsyncConnection.getTable(TableName.valueOf(hbaseNamespace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        CompletableFuture<Result> resultCompletableFuture = table.get(get);
        try {
            Result result = resultCompletableFuture.get();
            JSONObject dim = new JSONObject();
            for (Cell cell : result.rawCells()) {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                dim.put(column,value);
            }
            return dim;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;

    }
}
