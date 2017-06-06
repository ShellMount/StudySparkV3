package com.imqk.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SPARK SQL 内置函数
  * 内置函数的返回结果，是一个COLUM对象，并不是一个新的 DataFrame
  * DataFrame 是一个分布式形式存储的集合，更容易使用内置函数
  *
  * groupBy.agg
  * 还有
  * max,mean,min,sum,avg,explode,size,sort_array,
  * day,to_date,
  * abs,acros,asin,atan,
  *
  * 还有
  * 1，聚合函数： countDistinct, sumDistinct等
  * 2，集合函数： sorta_array, explode
  * 3, 日期函数： hour,quarter,next_day
  * 4, 数学函数： asin,atan,sqrt,tan,round
  * 5, 窗口函数： rowNumber
  * 6, 字符函数： concat, format_number,rexexp_extract
  * 7, 其它函数： isNan,sha,rand,callUDF,callUDAF
  *
  * Created by 428900 on 2017/5/12.
  */
object SparkSqlAgg {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("SparkSqlAgg")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    // 本地模式：sqlConntext
    val sqlConntext = new SQLContext(sc)

    // 集群/Hive 模式：sqlConntext
    // val sqlConntext = new HiveContext(sc)

    // 开启隐式转换, 否则不能使用内置函数
    import sqlConntext.implicits._

    // 构造数据
    val userData = Array(
      "2017-5-12,001,http://www.imqk.com,1000",
      "2017-5-12,001,http://www.kuvx.com,1050",
      "2017-5-12,002,http://www.xieyie.com,200",
      "2017-5-12,003,http://www.shellmount.com,50",
      "2017-5-13,003,http://www.ssxr.com,15",
      "2017-5-13,004,http://www.cncool.com,19999"
    )

    val userDataRDD = sc.parallelize(userData)

    // 生成DF ： RDD -> DF，需要先将RDD中的元素变为Row类型
    // 并且提供数据结构
    val userDataRDDRow = userDataRDD.map{row => {val splited = row.split(","); Row(splited(0), splited(1).toInt, splited(2), splited(3).toInt)}}

    // 产生结构
    val structType =StructType(Array(
      StructField("time", StringType, true),
      StructField("id", IntegerType, true),
      StructField("url", StringType, true),
      StructField("amount", IntegerType, true)
    ))

    // 创建DF
    val userDataDF = sqlConntext.createDataFrame(userDataRDDRow, structType)

    // 操作 数据：使用内置函数
    // 返回对象为 Column,且自行CG
    // 没有隐式转换，和下面的这一行，无法使用agg， 内置函数无法自动导入
    import org.apache.spark.sql.functions.{countDistinct, sum}
    userDataDF.groupBy("time").agg('time, countDistinct('id)).show()

    println ("==" *30 + "统计销售额度")
    // 统计销售额度
    userDataDF.groupBy("time").agg('time, sum('amount)).show()

  }

}















