package com.imqk.spark.api.java.Sql;

import java.sql.*;

/**
 * 用编程的方式，通过JDBC的方式访问 Thrift Server，进而访问HIVE数据，并处理数据
 * 这是企业中常用的方式
 * Thrift Server 是个桥梁，但它比存HIVE命令行中操作更快（如 count 等操作时）
 *
 * 跟我们平常访问 HIVE 及数据操作相当不同哟，连 SparkContext 都不需要了
 *
 * 本代码不能跑在本地。
 * Created by 428900 on 2017/5/12.
 */
public class SparSQLdbc2ThriftServer {
    public static void main(String[] args) {
        // 注意，使用JDBC连接服务器的时候，服务器端需用如下方式启动
        // $ ./start-thriftserver.sh --master spark://sparkmaster:7077 --hiveconf hive.server2.transport.mode=http --hiveconf hive.server2.thrift.http.path=cliservice

        String sqlText = "SELECT COUNT(*) FROM if0000 WHERE Open > ?";
        Connection conn = null;
        ResultSet resultSet = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://master:10001/datatick?" +
                            "hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice",
                    "root", "");
            PreparedStatement prepareStatement = conn.prepareStatement(sqlText);     // prepareStatement: 本方式连接可防SQL注入
            prepareStatement.setInt(1, 20);
            resultSet = prepareStatement.executeQuery();

            while(resultSet.next()){
                System.out.println(resultSet.getString(1));     // 此处的数据可保存到 parquet 中
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                resultSet.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
