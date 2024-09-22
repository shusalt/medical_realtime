package com.atguigu.medical.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.medical.realtime.app.func.AsyncDimFunctionHBase;
import com.atguigu.medical.realtime.bean.DwsInteractionHosReviewBean;
import com.atguigu.medical.realtime.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2023-07-24 11:34
 */
public class DwsInteractionHosReviewWin {
    public static void main(String[] args) throws Exception {
        // TODO 1 初始化流环境
        String name = "medical_dws_interaction_hos_review_win";
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8107, name);
        env.setParallelism(1);

        // TODO 2 读取dwd层数据
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaConsumer("medical_dwd_interaction_review", name);
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), name);

        // TODO 3 转换结构
        SingleOutputStreamOperator<DwsInteractionHosReviewBean> beanStream = streamSource.map(new MapFunction<String, DwsInteractionHosReviewBean>() {
            @Override
            public DwsInteractionHosReviewBean map(String value) throws Exception {
                DwsInteractionHosReviewBean bean = JSON.parseObject(value, DwsInteractionHosReviewBean.class);
                bean.setCurDate(DateFormatUtil.toDate(bean.getTs()));
                bean.setReviewCount(1L);
                bean.setGoodReviewCount(bean.getRating() == 5 ? 1L : 0L);
                return bean;
            }
        });

        // TODO 4 关联评价表中的医院id
        SingleOutputStreamOperator<DwsInteractionHosReviewBean> withHosIdStream = AsyncDataStream.unorderedWait(beanStream, new AsyncDimFunctionHBase<DwsInteractionHosReviewBean>() {
            @Override
            public String getTable() {
                return "dim_doctor";
            }

            @Override
            public String getId(DwsInteractionHosReviewBean bean) {
                return bean.getDoctorId();
            }

            @Override
            public void addDim(DwsInteractionHosReviewBean bean, JSONObject dim) {
                bean.setHospitalId(dim.getString("hospital_id"));
            }
        }, 60L, TimeUnit.SECONDS);

        // TODO 5 按照用户id和医院id进行分组
        KeyedStream<DwsInteractionHosReviewBean, String> userHosIdKeyedStream = withHosIdStream.keyBy(new KeySelector<DwsInteractionHosReviewBean, String>() {
            @Override
            public String getKey(DwsInteractionHosReviewBean value) throws Exception {
                return value.getUserId() + "-" + value.getHospitalId();
            }
        });

        // TODO 6 通过状态编程统计新增人数
        SingleOutputStreamOperator<DwsInteractionHosReviewBean> fullBeanStream = userHosIdKeyedStream.process(new KeyedProcessFunction<String, DwsInteractionHosReviewBean, DwsInteractionHosReviewBean>() {

            private ValueState<Boolean> isFirstState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                isFirstState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is_first", Boolean.class));
            }

            @Override
            public void processElement(DwsInteractionHosReviewBean bean, Context ctx, Collector<DwsInteractionHosReviewBean> out) throws Exception {
                Boolean isFirst = isFirstState.value();
                if (isFirst == null) {
                    // 当前为新增评价
                    bean.setReviewNewUserCount(1L);
                    isFirstState.update(false);
                } else {
                    bean.setReviewNewUserCount(0L);
                }
                out.collect(bean);
            }
        });

        // TODO 7 引入水位线
        SingleOutputStreamOperator<DwsInteractionHosReviewBean> withWaterMarkStream = fullBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsInteractionHosReviewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsInteractionHosReviewBean>() {
            @Override
            public long extractTimestamp(DwsInteractionHosReviewBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 8 分组医院id
        KeyedStream<DwsInteractionHosReviewBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsInteractionHosReviewBean, String>() {
            @Override
            public String getKey(DwsInteractionHosReviewBean value) throws Exception {
                return value.getHospitalId();
            }
        });

        // TODO 9 开窗
        WindowedStream<DwsInteractionHosReviewBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 10 聚合
        SingleOutputStreamOperator<DwsInteractionHosReviewBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsInteractionHosReviewBean>() {
            @Override
            public DwsInteractionHosReviewBean reduce(DwsInteractionHosReviewBean value1, DwsInteractionHosReviewBean value2) throws Exception {
                value1.setReviewNewUserCount(value1.getReviewNewUserCount() + value2.getReviewNewUserCount());
                value1.setReviewCount(value1.getReviewCount() + value2.getReviewCount());
                value1.setGoodReviewCount(value1.getGoodReviewCount() + value2.getGoodReviewCount());
                return value1;
            }
        }, new ProcessWindowFunction<DwsInteractionHosReviewBean, DwsInteractionHosReviewBean, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<DwsInteractionHosReviewBean> elements, Collector<DwsInteractionHosReviewBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                for (DwsInteractionHosReviewBean bean : elements) {
                    bean.setStt(stt);
                    bean.setEdt(edt);
                    out.collect(bean);
                }
            }
        });

        // TODO 11 补充维度信息 医院名称
        SingleOutputStreamOperator<DwsInteractionHosReviewBean> withHosNameStream = AsyncDataStream.unorderedWait(reduceStream, new AsyncDimFunctionHBase<DwsInteractionHosReviewBean>() {
            @Override
            public String getTable() {
                return "dim_hospital";
            }

            @Override
            public String getId(DwsInteractionHosReviewBean bean) {
                return bean.getHospitalId();
            }

            @Override
            public void addDim(DwsInteractionHosReviewBean bean, JSONObject dim) {
                bean.setHospitalName(dim.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);

        // TODO 12 写出到doris
        SingleOutputStreamOperator<String> jsonStringStream = withHosNameStream.map(Bean2JSONUtil::bean2JSON);
        jsonStringStream.sinkTo(DorisUtil.getDorisSink("medical_realtime.dws_interaction_hos_review_win","dws_interaction_hos_review_win"));

        // TODO 13 执行任务
        env.execute();
    }
}
