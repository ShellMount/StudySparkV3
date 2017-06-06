package com.imqk.spark.api.java.Sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

public class DataFrameOps {

    public static void main(String[] args) {
        // TODO 自动生成的方法存根
        SparkConf conf = new SparkConf();
        conf.setAppName("DataFrameOps");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        //创建 DataFrame, 其是一张表
        Dataset df = sqlContext.read().json("hdfs://hdmaster:9000/examples/Spark/examples/src/main/resources/people.json");

        df.show();

        df.printSchema();

        df.select("name").show();

        df.select(df.col("name"), df.col("age").plus(10)).show();

        df.select(df.col("age").gt(10)).show();

        df.groupBy(df.col("age")).count().show();

    }

}
