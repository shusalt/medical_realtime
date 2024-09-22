package com.atguigu.medical.realtime.util;

import com.atguigu.medical.realtime.common.MedicalCommon;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;

import java.util.Properties;

/**
 * @author yhm
 * @create 2023-07-23 10:41
 */
public class DorisUtil {
    public static DorisSink<String> getDorisSink(String table,String label){
        // 写入数据的格式
        Properties properties = new Properties();
        properties.setProperty("format","json");
        properties.setProperty("read_json_by_line","true");


        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(
                        // 连接参数
                        DorisOptions.builder()
                                .setFenodes(MedicalCommon.DORIS_FE_NODES)
                                .setTableIdentifier(table)
                                .setUsername(MedicalCommon.DORIS_USER_NAME)
                                .setPassword(MedicalCommon.DORIS_PASSWD)
                                .build()
                )
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .setLabelPrefix(label)
                        .setBufferSize(1024*1024)//默认1M
                        .setBufferCount(3) // 默认一批3条
                        .setCheckInterval(3000)
                        .setMaxRetries(3)
                        .disable2PC()// 2阶段提交 实际开发打开保证一致性
                        .setStreamLoadProp(properties) // 修改写入的文件格式 默认是csv 改成json
                        .setDeletable(false)
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}
