package com.atguigu.medical.realtime.bean;

import com.atguigu.medical.realtime.util.DateFormatUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author yhm
 * @create 2023-07-22 13:53
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwsTradeHospPatGenAGConsulBean {
    // 问诊 ID
    @TransientSink
    String id;

    // 问诊时间
    @TransientSink
    String createTime;

    // 诊金
    @TransientSink
    BigDecimal consultationFee;

    // 医生 ID
    @TransientSink
    String doctorId;

    // 医院 ID
    String hospitalId;

    // 医院名称
    String hospitalName;

    // 病人 ID
    @TransientSink
    String patientId;

    // 性别编码
    String patientGenderCode;

    // 性别名称
    String patientGender;

    // 年龄段
    String ageGroup;

    // 用户 ID
    @TransientSink
    String userId;

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 问诊金额
    BigDecimal consultationAmount;

    // 问诊次数
    Long consultationCount;

    public Long getTs() {
        return DateFormatUtil.toTs(createTime.substring(0, 19), true);
    }
}