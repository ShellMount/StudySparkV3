package com.imqk.spark.stream;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * Created by 428900 on 2017/5/18.
 */
public class ConnectionPool {
    // 下面一行的 private ，不能换成 public, 否则其它调用类中调用时，会提示找不到 ConnectionPool。
    // 为什么会这样
    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection() {
        if (connectionQueue == null){
            connectionQueue = new LinkedList<Connection>();
            for (int i = 0; i < 5; i++){
                try {
                    Connection conn = DriverManager.getConnection(
                            // 需要确保这里使用的 host,user,password 是可以访问该数据的
                            "jdbc:mysql://localhost:3306/sparkstreaming",
                            "root",
                            "manager!74"
                    );
                    connectionQueue.push(conn);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}
