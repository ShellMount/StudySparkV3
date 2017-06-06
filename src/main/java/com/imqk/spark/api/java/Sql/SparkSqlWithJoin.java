package com.imqk.spark.api.java.Sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 连接两个临时表 DF
 * 以下是读取数据，处理数据，保存数据的过程。
 * 在RDD与DF之间进行转换
 * 可以看出RDD适用于 map/reduce 等运算
 * DF数据适用于 SQL 及数据保存
 * Created by 428900 on 2017/5/9.
 */

public class SparkSqlWithJoin {

    public  static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("RDD2DataFrameByProgramming");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //JavaRDD<String> lines = sc.textFile("F:\\STUDY\\hadoop\\DOCUMENT\\people2.json");

        // 创建 DataFrame/ Dataset数据源
        Dataset peopleDF = sqlContext.read().json("F:\\STUDY\\hadoop\\DOCUMENT\\people2.json");

        // 注册表
        peopleDF.registerTempTable("peopleScore");

        // 查寻数据
        Dataset execScoreDF = sqlContext.sql("select name, score from peopleScore where score > 10");

        // DF 转 RDD: 将上面的结果，转为一个 RDD 结构，每条数据为一个名字list
        List<String> execNameScoreList = execScoreDF.javaRDD().map(new Function<Row, String>() {
            //@Override
            public String call(Row row) throws Exception {
                return row.getAs("name").toString();
            }
        }).collect();

        // 构造新JSON : 名字－年龄表
        List<String> peopleInfo = new ArrayList<String>();
        peopleInfo.add("{\"name\":\"Michael\", \"age\": 20}");
        peopleInfo.add("{\"name\":\"Andy\", \"age\":130}");
        peopleInfo.add("{\"name\":\"Justin\", \"age\":9000}");

        // 转换为 DF
        JavaRDD<String> peopleInfoRDD = sc.parallelize(peopleInfo);
        Dataset poepleInfoDF = sqlContext.read().json(peopleInfoRDD);

        // 注册为表：名字－年龄表
        poepleInfoDF.registerTempTable("peopleInfo");

        String sqlText = "select name, age from peopleInfo where name in (";
        for (int i = 0; i < execNameScoreList.size(); i++){
            sqlText += "'" + execNameScoreList.get(i) + "'" ;
            if (i < execNameScoreList.size() -1){
                sqlText += ",";
            }
        }
        sqlText += ")";

        System.out.println(sqlText);
        // 查出符合条件的数据：保留两个表中相同名字的人，及其年龄数据
        Dataset execNameAgeDF = sqlContext.sql(sqlText);

        // 生成 新的RDD：[(名字, (年龄, 分数)),...] //这是两个RDD的join
        JavaPairRDD<String, Tuple2<Integer, Integer>> resultRDD = execScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            public static final long serialVersionUID = 1L;

            //@Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getAs("name").toString(), Integer.valueOf(row.getAs("score").toString()));
            }
        }).join(execNameAgeDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            public static final long serialVersionUID = 1L;

            //@Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                // 字符串转 LONG，再转 Integer
                System.out.println((Long) row.getAs("age"));
                return new Tuple2<String, Integer>(row.getAs("name").toString(), Integer.valueOf(row.getAs("age").toString()));
            }
        }));

        // 对结果处理 转换为 [(名字, 年龄, 分数),...]
        JavaRDD<Row> resultRowRDD = resultRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

            //@Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
                return RowFactory.create(stringTuple2Tuple2._1, stringTuple2Tuple2._2._1, stringTuple2Tuple2._2._2);
            }
        });

        // 动态构造 DataFrame 的元数据
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        // 创建新的DF
        Dataset personDF = sqlContext.createDataFrame(resultRowRDD, structType);

        // 保存新生成的DF数据，也可以使用 parquet 保存数据
        personDF.write().format("json").save("F:\\STUDY\\OutPutApp\\sqpkSqlWithJoin.json");

        personDF.show();

    }

}
