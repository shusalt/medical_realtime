package com.atguigu.medical.realtime.bean;

import com.atguigu.medical.realtime.util.DateFormatUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwsDocRegisterBean {

    // 医生 ID
    @TransientSink
    String id;

    // 注册时间
    @TransientSink
    String createTime;

    // 生日
    @TransientSink
    String birthday;

    // 就诊费用
    @TransientSink
    String consultationFee;

    // 性别编码：101.男 102.女
    @TransientSink
    String gender;

    // 专业编码
    @TransientSink
    String specialty;

    // 职称编码
    @TransientSink
    String title;

    // 所属医院
    @TransientSink
    String hospitalId;

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当前日期
    String curDate;

    // 注册医生数
    Long registerCount;

    public Long getTs() {
        return DateFormatUtil.toTs(createTime.substring(0, 19), true);
    }
}
