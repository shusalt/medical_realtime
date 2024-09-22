package com.atguigu.medical.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.atguigu.medical.realtime.bean.DwsUserRegisterBean;
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
public class DwsUserRegisterWin {
    public static void main(String[] args) throws Exception {
        // TODO 1 初始化流环境
        String name = "medical_dws_user_register_win";
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8104, name);
        env.setParallelism(1);

        // TODO 2 读取dwd层数据
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaConsumer("medical_dwd_user_register", name);
        DataStreamSource<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), name);

        // TODO 3 转换结构
        SingleOutputStreamOperator<DwsUserRegisterBean> beanStream = sourceStream.map(new MapFunction<String, DwsUserRegisterBean>() {
            @Override
            public DwsUserRegisterBean map(String value) throws Exception {
                DwsUserRegisterBean bean = JSON.parseObject(value, DwsUserRegisterBean.class);
                bean.setCurDate(DateFormatUtil.toDate(bean.getTs()));
                bean.setRegisterUserCount(1L);
                return bean;
            }
        });

        // TODO 4 添加水位线
        SingleOutputStreamOperator<DwsUserRegisterBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsUserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsUserRegisterBean>() {
            @Override
            public long extractTimestamp(DwsUserRegisterBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 开窗
        AllWindowedStream<DwsUserRegisterBean, TimeWindow> windowStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 6 聚合
        SingleOutputStreamOperator<DwsUserRegisterBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsUserRegisterBean>() {
            @Override
            public DwsUserRegisterBean reduce(DwsUserRegisterBean value1, DwsUserRegisterBean value2) throws Exception {
                // 合并度量值
                value1.setRegisterUserCount(value1.getRegisterUserCount() + value2.getRegisterUserCount());
                return value1;
            }
        }, new ProcessAllWindowFunction<DwsUserRegisterBean, DwsUserRegisterBean, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<DwsUserRegisterBean> elements, Collector<DwsUserRegisterBean> out) throws Exception {
                // 添加开始结束窗口时间
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                for (DwsUserRegisterBean bean : elements) {
                    bean.setStt(stt);
                    bean.setEdt(edt);
                    out.collect(bean);
                }
            }
        });

        // TODO 7 写出到doris
        SingleOutputStreamOperator<String> jsonStringStream = reduceStream.map(Bean2JSONUtil::bean2JSON);
        jsonStringStream.sinkTo(DorisUtil.getDorisSink("medical_realtime.dws_user_register_win","dws_user_register_win"));

        // TODO 8 执行任务
        env.execute();

    }
}
