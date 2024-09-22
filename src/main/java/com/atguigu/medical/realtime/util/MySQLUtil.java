package com.atguigu.medical.realtime.util;

import com.atguigu.medical.realtime.common.MedicalCommon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author yhm
 * @create 2023-07-19 14:49
 */
public class MySQLUtil {
    public static Connection getConnection(){
        String url = MedicalCommon.MYSQL_URL;
        String username = MedicalCommon.MYSQL_USERNAME;
        String password = MedicalCommon.MYSQL_PASSWD;

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, username, password);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return connection;
    }
}
