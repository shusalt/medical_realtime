package com.atguigu.medical.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.medical.realtime.app.func.AsyncDimFunctionHBase;
import com.atguigu.medical.realtime.bean.DwsTradeHospPatGenAGPresBean;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2023-07-23 15:27
 */
public class DwsTradeHospPatGenAGPresWin {
    public static void main(String[] args) throws Exception {
        // TODO 1 初始化流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8102, "medical_dws_trade_hosp_pat_gen_a_g_pres_win");
        env.setParallelism(1);

        // TODO 2 读取dwd层数据
        String topicName = "medical_dwd_trade_prescription";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaConsumer(topicName, "medical_dws_trade_hosp_pat_gen_a_g_pres_win");
        DataStreamSource<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "medical_dws_trade_hosp_pat_gen_a_g_pres_win");

        // TODO 3 转换数据结构
        // 需要多处方订单进行去重
        // 按照prescriptionId分组
        SingleOutputStreamOperator<DwsTradeHospPatGenAGPresBean> beanStream = sourceStream.map(new MapFunction<String, DwsTradeHospPatGenAGPresBean>() {
            @Override
            public DwsTradeHospPatGenAGPresBean map(String value) throws Exception {
                DwsTradeHospPatGenAGPresBean bean = JSON.parseObject(value, DwsTradeHospPatGenAGPresBean.class);
                bean.setCurDate(DateFormatUtil.toDate(bean.getTs()));
                bean.setPrescriptionCount(1L);
                bean.setPrescriptionAmount(bean.getTotalAmount());
                return bean;
            }
        });

        KeyedStream<DwsTradeHospPatGenAGPresBean, String> beanKeyedStream = beanStream.keyBy(new KeySelector<DwsTradeHospPatGenAGPresBean, String>() {
            @Override
            public String getKey(DwsTradeHospPatGenAGPresBean bean) throws Exception {
                return bean.getPrescriptionId();
            }
        });

        SingleOutputStreamOperator<DwsTradeHospPatGenAGPresBean> isFirstStream = beanKeyedStream.process( new KeyedProcessFunction<String,DwsTradeHospPatGenAGPresBean, DwsTradeHospPatGenAGPresBean>() {

            private ValueState<Boolean> isFirstStatus = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                isFirstStatus = getRuntimeContext().<Boolean>getState(new ValueStateDescriptor<Boolean>("is_first", Boolean.class));
            }

            @Override
            public void processElement(DwsTradeHospPatGenAGPresBean value, Context ctx, Collector<DwsTradeHospPatGenAGPresBean> out) throws Exception {
                Boolean isFirst = isFirstStatus.value();
                if (isFirst == null) {
                    isFirstStatus.update(false);
                    out.collect(value);
                }
            }
        });


        // TODO 4 关联医院id
        SingleOutputStreamOperator<DwsTradeHospPatGenAGPresBean> withHospitalIdStream = AsyncDataStream.unorderedWait(isFirstStream, new AsyncDimFunctionHBase<DwsTradeHospPatGenAGPresBean>() {
            @Override
            public String getTable() {
                return "dim_doctor";
            }

            @Override
            public String getId(DwsTradeHospPatGenAGPresBean bean) {
                return bean.getDoctorId();
            }

            @Override
            public void addDim(DwsTradeHospPatGenAGPresBean bean, JSONObject dim) {
                bean.setHospitalId(dim.getString("hospital_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // TODO 5 关联获得病人信息
        SingleOutputStreamOperator<DwsTradeHospPatGenAGPresBean> withGenAggStream = AsyncDataStream.unorderedWait(withHospitalIdStream, new AsyncDimFunctionHBase<DwsTradeHospPatGenAGPresBean>() {
            @Override
            public String getTable() {
                return "dim_patient";
            }

            @Override
            public String getId(DwsTradeHospPatGenAGPresBean bean) {
                return bean.getPatientId();
            }

            @Override
            public void addDim(DwsTradeHospPatGenAGPresBean bean, JSONObject dim) {
                bean.setPatientGenderCode(dim.getString("gender"));
                String birthday = dim.getString("birthday");
                LocalDate birDt = LocalDate.parse(birthday, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                LocalDate currentDt = LocalDate.now();
                int age = Period.between(birDt, currentDt).getYears();
                String ageGroup = null;
                if (age >= 0 && age <= 2) {
                    ageGroup = "婴儿期";
                } else if (age <= 5) {
                    ageGroup = "幼儿期";
                } else if (age <= 11) {
                    ageGroup = "小学阶段";
                } else if (age <= 17) {
                    ageGroup = "青少年期";
                } else if (age <= 29) {
                    ageGroup = "青年期";
                } else if (age <= 59) {
                    ageGroup = "中年期";
                } else if (age > 60) {
                    ageGroup = "老年期";
                } else {
                    ageGroup = "年龄异常";
                }
                bean.setAgeGroup(ageGroup);
            }
        }, 60, TimeUnit.SECONDS);

        // TODO 6 引入水位线
        SingleOutputStreamOperator<DwsTradeHospPatGenAGPresBean> withWaterMarkStream = withGenAggStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTradeHospPatGenAGPresBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeHospPatGenAGPresBean>() {
            @Override
            public long extractTimestamp(DwsTradeHospPatGenAGPresBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 7 按照医院id,患者性别,患者年龄分组
        KeyedStream<DwsTradeHospPatGenAGPresBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsTradeHospPatGenAGPresBean, String>() {
            @Override
            public String getKey(DwsTradeHospPatGenAGPresBean value) throws Exception {

                return value.getHospitalId() + "-" + value.getPatientGenderCode() + "-" + value.getAgeGroup();
            }
        });

        // TODO 8 开窗
        WindowedStream<DwsTradeHospPatGenAGPresBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 9 聚合
        SingleOutputStreamOperator<DwsTradeHospPatGenAGPresBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsTradeHospPatGenAGPresBean>() {
            @Override
            public DwsTradeHospPatGenAGPresBean reduce(DwsTradeHospPatGenAGPresBean value1, DwsTradeHospPatGenAGPresBean value2) throws Exception {
                value1.setPrescriptionAmount(value1.getPrescriptionAmount().add(value2.getPrescriptionAmount()));
                value1.setPrescriptionCount(value1.getPrescriptionCount() + value2.getPrescriptionCount());
                return value1;
            }
        }, new WindowFunction<DwsTradeHospPatGenAGPresBean, DwsTradeHospPatGenAGPresBean, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<DwsTradeHospPatGenAGPresBean> input, Collector<DwsTradeHospPatGenAGPresBean> out) throws Exception {
                for (DwsTradeHospPatGenAGPresBean bean : input) {
                    String stt = DateFormatUtil.toYmdHms(window.getStart());
                    String edt = DateFormatUtil.toYmdHms(window.getEnd());

                    bean.setStt(stt);
                    bean.setEdt(edt);
                    out.collect(bean);
                }
            }
        });

        // TODO 10 补充维度信息
        // 10.1 补充医院名称
        SingleOutputStreamOperator<DwsTradeHospPatGenAGPresBean> withHosNameStream = AsyncDataStream.unorderedWait(reduceStream, new AsyncDimFunctionHBase<DwsTradeHospPatGenAGPresBean>() {
            @Override
            public String getTable() {
                return "dim_hospital";
            }

            @Override
            public String getId(DwsTradeHospPatGenAGPresBean bean) {
                return bean.getHospitalId();
            }

            @Override
            public void addDim(DwsTradeHospPatGenAGPresBean bean, JSONObject dim) {
                bean.setHospitalName(dim.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);

        // 10.2 补充性别名称
        SingleOutputStreamOperator<DwsTradeHospPatGenAGPresBean> valueStream = AsyncDataStream.unorderedWait(withHosNameStream, new AsyncDimFunctionHBase<DwsTradeHospPatGenAGPresBean>() {
            @Override
            public String getTable() {
                return "dim_dict";
            }

            @Override
            public String getId(DwsTradeHospPatGenAGPresBean bean) {
                return bean.getPatientGenderCode();
            }

            @Override
            public void addDim(DwsTradeHospPatGenAGPresBean bean, JSONObject dim) {
                bean.setHospitalName(dim.getString("value"));
            }
        }, 60, TimeUnit.SECONDS);

        // TODO 11 写出到doris
        SingleOutputStreamOperator<String> jsonStringStream = valueStream.map(Bean2JSONUtil::bean2JSON);
        jsonStringStream.sinkTo(DorisUtil.getDorisSink("medical_realtime.dws_trade_hosp_pat_gen_a_g_pres_win","dws_trade_hosp_pat_gen_a_g_pres_win"));

        // TODO 12 执行任务
        env.execute();
    }
}
