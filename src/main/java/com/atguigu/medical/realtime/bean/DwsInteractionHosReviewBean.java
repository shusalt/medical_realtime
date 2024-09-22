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
public class DwsInteractionHosReviewBean {
    // 评价ID
    @TransientSink
    String id;

    // 评价时间
    @TransientSink
    String reviewTime;

    // 评分
    @TransientSink
    Integer rating;

    // 医生ID
    @TransientSink
    String doctorId;

    // 医院ID
    String hospitalId;

    // 医院名称
    String hospitalName;

    // 患者ID
    @TransientSink
    String patientId;

    // 用户ID
    @TransientSink
    String userId;

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 新增评价人数
    Long reviewNewUserCount;

    // 评价次数
    Long reviewCount;

    // 好评次数
    Long goodReviewCount;

    public Long getTs() {
        return DateFormatUtil.toTs(reviewTime.substring(0, 19), true);
    }
}
