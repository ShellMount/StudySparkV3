package com.imqk.spark.sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 428900 on 2017/5/9.
  */

object RDD2DataFrameByProgramming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("RDD2DataFrameReflection By Scala")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // 导入 sqlContext 的隐式转换

    val lines = sc.textFile("F:\\STUDY\\hadoop\\DOCUMENT\\person.txt")

    val schemaString = Array("id", "name", "age")

    val schema = StructType(schemaString.map{fieldName => StructField(fieldName, StringType, true)})
    val rowRDD = lines.map{_.split(",")}.map{splited => Row( splited(0), splited(1), splited(2) )}
    val peopleDF = sqlContext.createDataFrame(rowRDD, schema)
    peopleDF.registerTempTable("people")

    val result = sqlContext.sql("select age from people where name == 'Hadoop'")
    // 上面的构造，不能使用 where age ...
    //val result = sqlContext.sql("select * from people where age > 8")
    result.collect.foreach(println)

  }
}
