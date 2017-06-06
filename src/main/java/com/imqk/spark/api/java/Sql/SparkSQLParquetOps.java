package com.imqk.spark.api.java.Sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Created by 428900 on 2017/5/10.
 */
public class SparkSQLParquetOps {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkSQLLoadSavaOps");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset userDF = sqlContext.read().parquet("F:\\STUDY\\spark\\examples\\src\\main\\resources\\users.parquet");

        userDF.registerTempTable("user");

        Dataset result = sqlContext.sql("select * from user");

        // 对数据进行操作
        JavaRDD<String> resultRDD = result.javaRDD().map(new Function<Row, String>() {
            //@Override
            public String call(Row row) throws Exception {
                return "YES" + row.getAs("name");
            }
        });

        List<String> listRow = resultRDD.collect();

        for (String row: listRow) {
            System.out.println(row);
        }
    }
}
