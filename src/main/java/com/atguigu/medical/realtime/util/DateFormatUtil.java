package com.atguigu.medical.realtime.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author yhm
 * @create 2023-07-22 13:56
 */
public class DateFormatUtil {


    // 定义 yyyy-MM-dd 格式的日期格式化对象
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // 定义 yyyy-MM-dd HH:mm:ss 格式的日期格式化对象
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将格式化日期字符串转换为时间
     *
     * @param dtStr  格式化日期字符串
     * @param isFull 标记，表示日期字符串是否包含 HH:mm:ss 部分
     * @return 格式化日期字符串转换得到的时间戳
     */
    public static Long toTs(String dtStr, boolean isFull) {

        // 定义日期对象
        LocalDateTime localDateTime = null;
        // 判断日期字符串是否包含 HH:mm:ss 部分
        if (!isFull) {
            // 日期字符串不全，补充 HH:mm:ss 部分
            dtStr = dtStr + " 00:00:00";
        }
        // 将格式化日期字符串转换为 LocalDateTime 类型的日期对象
        localDateTime = LocalDateTime.parse(dtStr, dtfFull);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    /**
     * 将不包含 HH:mm:ss 部分的格式化日期字符串转换为时间戳
     *
     * @param dtStr 格式化日期字符串
     * @return 转换后的时间戳
     */
    public static Long toTs(String dtStr) {
        return toTs(dtStr, false);
    }

    /**
     * 将时间戳转换为 yyyy-MM-dd 格式的格式化日期字符串
     *
     * @param ts 时间戳
     * @return 格式化日期字符串
     */
    public static String toDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    /**
     * 将时间戳转换为 yyyy-MM-dd HH:mm:ss 格式的格式化日期字符串
     *
     * @param ts 时间戳
     * @return 格式化日期字符串
     */
    public static String toYmdHms(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }
}
