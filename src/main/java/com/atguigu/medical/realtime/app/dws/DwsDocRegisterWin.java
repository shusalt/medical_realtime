package com.atguigu.medical.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.atguigu.medical.realtime.bean.DwsDocRegisterBean;
import com.atguigu.medical.realtime.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yhm
 * @create 2023-07-23 17:13
 */
public class DwsDocRegisterWin {
    public static void main(String[] args) throws Exception {
        // TODO 1 初始化流环境
        String name = "medical_dws_doc_register_win";
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8106, name);
        env.setParallelism(1);

        // TODO 2 读取dwd层数据
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaConsumer("medical_dwd_doctor_register", name);
        DataStreamSource<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), name);

        // TODO 3 转换结构
        SingleOutputStreamOperator<DwsDocRegisterBean> beanStream = sourceStream.map(new MapFunction<String, DwsDocRegisterBean>() {
            @Override
            public DwsDocRegisterBean map(String value) throws Exception {
                DwsDocRegisterBean bean = JSON.parseObject(value, DwsDocRegisterBean.class);
                bean.setCurDate(DateFormatUtil.toDate(bean.getTs()));
                bean.setRegisterCount(1L);
                return bean;
            }
        });

        // TODO 4 添加水位线
        SingleOutputStreamOperator<DwsDocRegisterBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsDocRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsDocRegisterBean>() {
            @Override
            public long extractTimestamp(DwsDocRegisterBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 开窗
        AllWindowedStream<DwsDocRegisterBean, TimeWindow> windowStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 6 聚合
        SingleOutputStreamOperator<DwsDocRegisterBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsDocRegisterBean>() {
            @Override
            public DwsDocRegisterBean reduce(DwsDocRegisterBean value1, DwsDocRegisterBean value2) throws Exception {
                // 合并度量值
                value1.setRegisterCount(value1.getRegisterCount() + value2.getRegisterCount());
                return value1;
            }
        }, new ProcessAllWindowFunction<DwsDocRegisterBean, DwsDocRegisterBean, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<DwsDocRegisterBean> elements, Collector<DwsDocRegisterBean> out) throws Exception {
                // 添加开始结束窗口时间
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                for (DwsDocRegisterBean bean : elements) {
                    bean.setStt(stt);
                    bean.setEdt(edt);
                    out.collect(bean);
                }
            }
        });

        // TODO 7 写出到doris
        SingleOutputStreamOperator<String> jsonStringStream = reduceStream.map(Bean2JSONUtil::bean2JSON);
        jsonStringStream.sinkTo(DorisUtil.getDorisSink("medical_realtime.dws_doc_register_win","dws_doc_register_win"));

        // TODO 8 执行任务
        env.execute();

    }
}
