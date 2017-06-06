package com.imqk.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 428900 on 2017/5/9.
  */
object DataFrame {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    conf.setAppName("DataFrame")
    conf.setMaster("spark://sparkmaster:7077")
    //conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val filePath = "hdfs://hdmaster:9000/examples/Spark/examples/src/main/resources/people.json"
    val df = sqlContext.read.json(filePath)

    df.show()

    df.printSchema()

    df.select("name").show()

    df.select(df("name"), df("age") + 10).show()

    df.filter(df("age") > 10).show()

    df.groupBy("age").count.show()

  }
}
