package com.imqk.spark.api.java.Sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by 428900 on 2017/5/9.
 */

public class RDD2DataFrameByProgramming {

    public  static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("RDD2DataFrameByProgramming");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("F:\\STUDY\\hadoop\\DOCUMENT\\person.txt");
        JavaRDD<Row> personsRDD = lines.map(new Function<String, Row>() {
            //@Override
            public Row call(String line) throws Exception {
                String[] splited = line.split(",");
                return RowFactory.create(Integer.valueOf(splited[0]), splited[1], Integer.valueOf(splited[2]));
            }
        });

        // 动态构造 DataFrame 的元数据
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        // 构造DataFrame
        Dataset personsDF = sqlContext.createDataFrame(personsRDD, structType);

        // 注册为数据表
        personsDF.registerTempTable("persons");

        // 数据分析
        Dataset result = sqlContext.sql("select * from persons where age > 8");

        // 结果处理
        List<Row> listRow = result.javaRDD().collect();

        for (Row row: listRow){
            System.out.println(row);
        }
    }

}
