package com.imqk.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 未完成的功能
  * Created by 428900 on 2017/5/9.
  */

//case class 要放在main方法外面
case class Person(id: Int, name: String, age: Int)

case class Person2(line: String, var age: Int = 0){
    // age 需要在 select 的 where 中使用，因此必须在上面的定义结构中存在
    // 下面的处理结果，会让数据结构中，多一个元素。
    // [3,Flink,300,310]
    val Array(id_, name_, age_) = line.split(",")
    age = age_.trim.toInt + 10
}

object RDD2DataFrameReflection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("RDD2DataFrameReflection By Scala")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // 导入 sqlContext 的隐式转换
    import sqlContext.implicits._

    val lines = sc.textFile("F:\\STUDY\\hadoop\\DOCUMENT\\person.txt")
    //val dfRDD = lines.map{line => line.split(",")}.map{splited => Person(splited(0).trim.toInt, splited(1), splited(2).trim.toInt)}
    val dfRDD = lines.map{line => Person2(line)}
    // 没有隐式转换，就没有这里的 toDF()
    val df = dfRDD.toDF()

    df.registerTempTable("persons")

    val bigData = sqlContext.sql("select * from persons where age > 2")

    val personList = bigData.collect()

    println("==" * 30 + "下面是结果:")
    personList.foreach(println)

    for (p <- personList){
      println ("--" + p.getClass)
      println (p)
    }
  }
}
