package com.imqk.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by 428900 on 2017/5/10.
  */
object ParquetMeger {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("ParquetMeger")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 增加隐式转换
    import sqlContext.implicits._
    val squaresDF = sc.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.parquet("F:\\STUDY\\OutPutApp\\meger\\key=1")

    val cubesDF = sc.makeRDD(1 to 5).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.parquet("F:\\STUDY\\OutPutApp\\meger\\key=2")

    val megedDF = sqlContext.read.option("mergeSchema", "true").parquet("F:\\STUDY\\OutPutApp\\meger")

    megedDF.printSchema()

    megedDF.show()

    megedDF.registerTempTable("times")

    val result = sqlContext.sql("select * from times")

    result.collect().foreach(println)


  }
}
