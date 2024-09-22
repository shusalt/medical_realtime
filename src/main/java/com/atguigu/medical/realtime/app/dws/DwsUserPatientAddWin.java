package com.atguigu.medical.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.atguigu.medical.realtime.bean.DwsUserPatientAddBean;
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
 * @create 2023-07-24 10:37
 */
public class DwsUserPatientAddWin {
    public static void main(String[] args) throws Exception {
        // TODO 1 初始化流环境
        String name = "medical_dws_user_patient_add_win";
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8105, name);
        env.setParallelism(1);

        // TODO 2 读取dwd层数据
        String topicName = "medical_dwd_user_patient_add";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaConsumer(topicName, name);
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), name);


        // TODO 3 转换结构
        SingleOutputStreamOperator<DwsUserPatientAddBean> beanStream = streamSource.map(new MapFunction<String, DwsUserPatientAddBean>() {
            @Override
            public DwsUserPatientAddBean map(String value) throws Exception {
                DwsUserPatientAddBean bean = JSON.parseObject(value, DwsUserPatientAddBean.class);
                bean.setCurDate(DateFormatUtil.toDate(bean.getTs()));
                bean.setPatientAddCount(1L);
                return bean;
            }
        });

        // TODO 4 引入水位线
        SingleOutputStreamOperator<DwsUserPatientAddBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsUserPatientAddBean>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<DwsUserPatientAddBean>() {
            @Override
            public long extractTimestamp(DwsUserPatientAddBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));


        // TODO 5 开窗
        AllWindowedStream<DwsUserPatientAddBean, TimeWindow> windowedStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));


        // TODO 6 聚合
        SingleOutputStreamOperator<DwsUserPatientAddBean> reduceStream = windowedStream.reduce(new ReduceFunction<DwsUserPatientAddBean>() {
            @Override
            public DwsUserPatientAddBean reduce(DwsUserPatientAddBean value1, DwsUserPatientAddBean value2) throws Exception {
                value1.setPatientAddCount(value1.getPatientAddCount() + value2.getPatientAddCount());
                return value1;
            }
        }, new ProcessAllWindowFunction<DwsUserPatientAddBean, DwsUserPatientAddBean, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<DwsUserPatientAddBean> elements, Collector<DwsUserPatientAddBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                for (DwsUserPatientAddBean bean : elements) {
                    bean.setStt(stt);
                    bean.setEdt(edt);
                    out.collect(bean);
                }
            }
        });


        // TODO 7 写出到doris
        SingleOutputStreamOperator<String> jsonStringStream = reduceStream.map(Bean2JSONUtil::bean2JSON);

        jsonStringStream.sinkTo(DorisUtil.getDorisSink("medical_realtime.dws_user_patient_add_win","dws_user_patient_add_win"));

        // TODO 8 执行任务
        env.execute();
    }
}
