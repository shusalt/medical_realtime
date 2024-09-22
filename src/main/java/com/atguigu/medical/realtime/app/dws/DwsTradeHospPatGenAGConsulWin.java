package com.atguigu.medical.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.medical.realtime.app.func.AsyncDimFunctionHBase;
import com.atguigu.medical.realtime.bean.DwsTradeHospPatGenAGConsulBean;
import com.atguigu.medical.realtime.util.*;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
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
 * @create 2023-07-22 13:30
 */
public class DwsTradeHospPatGenAGConsulWin {
    public static void main(String[] args) throws Exception {
        // TODO 1 初始化流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8100, "medical_dws_trade_hosp_pat_gen_a_g_consul_win");
        env.setParallelism(1);

        // TODO 2 读取kafka的dwd层对应数据
        String topicName = "medical_dwd_trade_consultation";
        String groupId = "medical_dws_trade_hosp_pat_gen_a_g_consul_win";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaConsumer(topicName, groupId);
        DataStreamSource<String> consultationStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), groupId);

        // TODO 3 转换对应的结构
        SingleOutputStreamOperator<DwsTradeHospPatGenAGConsulBean> beanStream = consultationStream.map(new MapFunction<String, DwsTradeHospPatGenAGConsulBean>() {
            @Override
            public DwsTradeHospPatGenAGConsulBean map(String value) throws Exception {
                DwsTradeHospPatGenAGConsulBean bean = JSONObject.parseObject(value, DwsTradeHospPatGenAGConsulBean.class);
                bean.setConsultationCount(1L);
                bean.setConsultationAmount(bean.getConsultationFee());
                bean.setCurDate(DateFormatUtil.toDate(bean.getTs()));
                return bean;
            }
        });

        // TODO 4 关联医院信息
        // 此处需要进行维度关联
        // 如果单条数据每次都访问一次hbase  效率非常低  需要进行优化
        // (1) 异步访问 -> 需要使用hbase的异步客户端 flink也需要使用异步算子
        // (2) 旁路缓存 -> 使用redis存储上一次访问的数据  下次访问的时候优先到redis中去读 如果没有需要的维度数据再到hbase中读
        SingleOutputStreamOperator<DwsTradeHospPatGenAGConsulBean> withHospitalIdStream = AsyncDataStream.unorderedWait(beanStream, new AsyncDimFunctionHBase<DwsTradeHospPatGenAGConsulBean>() {
            @Override
            public String getTable() {
                return "dim_doctor";
            }

            @Override
            public String getId(DwsTradeHospPatGenAGConsulBean bean) {
                return bean.getDoctorId();
            }

            @Override
            public void addDim(DwsTradeHospPatGenAGConsulBean bean, JSONObject dim) {
                bean.setHospitalId(dim.getString("hospital_id"));
            }
        }, 60, TimeUnit.SECONDS);

//        withHospitalIdStream.print(">>");


        // TODO 5 关联患者信息
        SingleOutputStreamOperator<DwsTradeHospPatGenAGConsulBean> withPatientStream = AsyncDataStream.unorderedWait(withHospitalIdStream, new AsyncDimFunctionHBase<DwsTradeHospPatGenAGConsulBean>() {
            @Override
            public String getTable() {
                return "dim_patient";
            }

            @Override
            public String getId(DwsTradeHospPatGenAGConsulBean bean) {
                return bean.getPatientId();
            }

            @Override
            public void addDim(DwsTradeHospPatGenAGConsulBean bean, JSONObject dim) {
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

        // TODO 6 添加水位线
        SingleOutputStreamOperator<DwsTradeHospPatGenAGConsulBean> withWaterMarkStream = withPatientStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTradeHospPatGenAGConsulBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeHospPatGenAGConsulBean>() {
            @Override
            public long extractTimestamp(DwsTradeHospPatGenAGConsulBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 7 按照医院id 性别  年龄进行分组
        KeyedStream<DwsTradeHospPatGenAGConsulBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsTradeHospPatGenAGConsulBean, String>() {
            @Override
            public String getKey(DwsTradeHospPatGenAGConsulBean value) throws Exception {
                return value.getHospitalId() + "-" + value.getPatientGenderCode() + "-" + value.getAgeGroup();
            }
        });

        // TODO 8 开窗
        WindowedStream<DwsTradeHospPatGenAGConsulBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 9 聚合
        SingleOutputStreamOperator<DwsTradeHospPatGenAGConsulBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsTradeHospPatGenAGConsulBean>() {
                                                                                                    @Override
                                                                                                    public DwsTradeHospPatGenAGConsulBean reduce(DwsTradeHospPatGenAGConsulBean value1, DwsTradeHospPatGenAGConsulBean value2) throws Exception {

                                                                                                        value1.setConsultationCount(value1.getConsultationCount() + value2.getConsultationCount());
                                                                                                        value1.setConsultationAmount(value1.getConsultationAmount().add(value2.getConsultationAmount()));
                                                                                                        return value1;
                                                                                                    }
                                                                                                },
                // 补充窗口的开始时间和结束时间
                new WindowFunction<DwsTradeHospPatGenAGConsulBean, DwsTradeHospPatGenAGConsulBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<DwsTradeHospPatGenAGConsulBean> input, Collector<DwsTradeHospPatGenAGConsulBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(window.getStart());
                        String edt = DateFormatUtil.toYmdHms(window.getEnd());
                        for (DwsTradeHospPatGenAGConsulBean bean : input) {
                            bean.setStt(stt);
                            bean.setEdt(edt);
                            out.collect(bean);
                        }
                    }
                });


        // TODO 10 补充缺失的维度信息
        // 10.1 补充医院名称
        SingleOutputStreamOperator<DwsTradeHospPatGenAGConsulBean> nameStream = AsyncDataStream.unorderedWait(reduceStream, new AsyncDimFunctionHBase<DwsTradeHospPatGenAGConsulBean>() {
            @Override
            public String getTable() {
                return "dim_hospital";
            }

            @Override
            public String getId(DwsTradeHospPatGenAGConsulBean bean) {
                return bean.getHospitalId();
            }

            @Override
            public void addDim(DwsTradeHospPatGenAGConsulBean bean, JSONObject dim) {
                bean.setHospitalName(dim.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);

        // 10.2 补充性别名称
        SingleOutputStreamOperator<DwsTradeHospPatGenAGConsulBean> resultStream = AsyncDataStream.unorderedWait(nameStream, new AsyncDimFunctionHBase<DwsTradeHospPatGenAGConsulBean>() {
            @Override
            public String getTable() {
                return "dim_dict";
            }

            @Override
            public String getId(DwsTradeHospPatGenAGConsulBean bean) {
                return bean.getPatientGenderCode();
            }

            @Override
            public void addDim(DwsTradeHospPatGenAGConsulBean bean, JSONObject dim) {
                bean.setPatientGender(dim.getString("value"));
            }
        }, 60, TimeUnit.SECONDS);



        // TODO 11 写出到doris
        // doris写入数据使用json字符串即可 kay的格式为蛇形
        SingleOutputStreamOperator<String> jsonStrStream = resultStream.map(new MapFunction<DwsTradeHospPatGenAGConsulBean, String>() {
            @Override
            public String map(DwsTradeHospPatGenAGConsulBean value) throws Exception {
                return Bean2JSONUtil.bean2JSON(value);
            }
        });

        jsonStrStream.print(">>>");

        jsonStrStream.sinkTo(DorisUtil.getDorisSink("medical_realtime.dws_trade_hosp_pat_gen_a_g_consul_win","dws_trade_hosp_pat_gen_a_g_consul_win"));


        // TODO 12 执行任务
        env.execute();
    }
}
