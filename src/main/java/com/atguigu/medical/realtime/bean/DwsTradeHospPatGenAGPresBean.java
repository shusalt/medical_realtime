package com.atguigu.medical.realtime.bean;

import com.atguigu.medical.realtime.util.DateFormatUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwsTradeHospPatGenAGPresBean {
    // 处方明细 ID
    @TransientSink
    String id;

    // 处方开具时间
    @TransientSink
    String prescriptionTime;

    // 剂量
    @TransientSink
    Integer count;

    // 药品 ID
    @TransientSink
    String medicineId;

    // 处方 ID
    @TransientSink
    String prescriptionId;

    // 处方总金额
    @TransientSink
    BigDecimal totalAmount;

    // 问诊 ID
    @TransientSink
    String consultationId;

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

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 处方开单金额
    BigDecimal prescriptionAmount;

    // 处方开单次数
    Long prescriptionCount;

    public Long getTs() {
        return DateFormatUtil.toTs(prescriptionTime.substring(0, 19), true);
    }
}