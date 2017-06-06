package com.imqk.spark.api.java.Sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

/**
 * Created by 428900 on 2017/5/10.
 */
public class SparkSQLLoadSavaOps {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkSQLLoadSavaOps");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 这里本应该是 DataFrame
        // 但它不能成功，启用 Dataset
        // Dataset peopleDF = sqlContext.read().format("json").load("F:\\STUDY\\spark\\examples\\src\\main\\resources\\people.json");
        // peopleDF.select("name").write().format("json").save("F:\\STUDY\\OutPutApp\\saveDataFrameAndSet.txt");
        // 如果早有文件，应该使用追加模式
        // peopleDF.select("name").write().mode(SaveMode.Append).save("F:\\STUDY\\OutPutApp\\saveDataFrameAndSet.txt");

        Dataset peopleDF = sqlContext.read().load("F:\\STUDY\\spark\\examples\\src\\main\\resources\\users.parquet");
        peopleDF.select("name").write().save("F:\\STUDY\\OutPutApp\\saveDataFrameAndSet.txt");


    }
}



