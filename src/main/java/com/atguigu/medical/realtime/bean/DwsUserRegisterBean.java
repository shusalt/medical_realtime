package com.atguigu.medical.realtime.bean;

import com.atguigu.medical.realtime.util.DateFormatUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2023-07-23 17:12
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwsUserRegisterBean {
    // 用户 ID
    @TransientSink
    String id;

    // 注册时间
    @TransientSink
    String registerTime;

    // 邮箱地址
    @TransientSink
    String email;

    // 电话号码
    @TransientSink
    String telephone;

    // 用户名称
    @TransientSink
    String username;

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 注册用户数
    Long registerUserCount;

    public Long getTs() {
        return DateFormatUtil.toTs(registerTime.substring(0, 19), true);
    }
}
