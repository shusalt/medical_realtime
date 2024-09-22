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
public class DwsUserPatientAddBean {
    // 患者ID
    @TransientSink
    String id;

    // 添加时间
    @TransientSink
    String addTime;

    // 生日
    @TransientSink
    String birthday;

    // 性别编码
    @TransientSink
    String genderCode;

    // 患者姓名（已脱敏）
    @TransientSink
    String name;

    // 所属用户ID
    @TransientSink
    String userId;

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 添加患者数
    Long patientAddCount;

    public Long getTs() {
        return DateFormatUtil.toTs(addTime.substring(0, 19), true);
    }
}
