package com.atguigu.medical.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2023-07-19 10:43
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcess {
    public String sourceTable;
    public String sinkTable;
    public String sinkFamily;
    public String sinkColumns;
    public String sinkRowKey;
    public String operateType;
}
