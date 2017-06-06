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
 * Created by 428900 on 2017/5/9.
 */
public class RDD2DataFrameReflection {

    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("RDD2DataFrameReflection");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("F:\\STUDY\\hadoop\\DOCUMENT\\person.txt");

        JavaRDD<Person> persons = lines.map(new Function<String, Person>() {
            //@Override
            public Person call(String line) throws Exception{
                String[] splited = line.split(",");
                Person p = new Person();
                p.setId(Integer.valueOf(splited[0].trim()));
                p.setAge(Integer.valueOf(splited[2].trim()));

                return p;
            }
        });

        Dataset df = sqlContext.createDataFrame(persons, Person.class);

        df.registerTempTable("persons");

        Dataset bigDatas = sqlContext.sql("select * from persons where age >= 6");
        JavaRDD<Row> bigDataRDD = bigDatas.javaRDD();
        JavaRDD<Person> result = bigDataRDD.map(new Function<Row, Person>() {
            //@Override
            public Person call(Row row) throws Exception{
                Person p = new Person();
                p.setId(row.getInt(1));
                p.setName(row.getString(2));
                p.setAge(row.getInt(0));
                return p;
            }
        });

        List<Person> personList = result.collect();

        for (Person p : personList){
            System.out.println(p);
        }
    }
}

