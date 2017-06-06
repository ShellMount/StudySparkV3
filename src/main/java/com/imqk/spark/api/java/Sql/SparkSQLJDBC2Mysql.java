package com.imqk.spark.api.java.Sql;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

/**
 * SPark SQL 访问 MYSQL 等关系型数据库
 * 如何访问 NoSQL 数据？
 * Created by 428900 on 2017/5/11.
 */
public class SparkSQLJDBC2Mysql {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkSQLJDBC2Mysql");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 通过 format jdbc 设置数据来源：ORACLE、MYSQL等
        DataFrameReader reader = sqlContext.read().format("jdbc");

        // 配置数据库信息
        reader.option("url", "jdbc:mysql://master:3306/future");
        //此外采用XSHELL TUNNEL映射的方式连接远程数据库
        reader.option("url", "jdbc:mysql://localhost:3306/future");

        reader.option("driver", "com.mysql.jdbc.Driver");
        reader.option("user", "root");
        reader.option("password", "manager!74");
        reader.option("dbtable", "if0000");

        // 加载数据, 旧版本下面的 Dataset 应该 为 DataFrame
        Dataset mysqlDataDF_IF0000 = reader.load();

        mysqlDataDF_IF0000.show();

        // 另一张表
        // reader.option("dbtable", "if0001");
        // Dataset mysqlDataDF_IF0001 = reader.load();

        // 对两张表的操作，参考 SparkSqlWithJoin.java  / SparkSqlWithJoin.scala
        String resutlDF = "数据处理过程";

        // 写入数据库：需将数据转RDD再写入数据库中
        // 下面的 DF 应为 resutlDF

        // 后文的插入数据库部分，不执行
        System.exit(0);

        mysqlDataDF_IF0000.javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {

            public static final long serialVersionUID = 1L;

            //@Override
            public void call(Iterator<Row> t) throws Exception {
                Connection conn2MySql = null;
                Statement statement = null;
                //String sqlText = "insert into ...";
                // 应该为上面的一行
                String sqlText = "select * from mysqlDataDF_IF0000";

                try {
                    conn2MySql = DriverManager.getConnection("jdbc:mysql://master:3306/future", "root", "manager!74");
                    statement = conn2MySql.createStatement();
                    statement.execute(sqlText);
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    if (conn2MySql != null) conn2MySql.close();
                }

            }
        });


    }
}

