package com.imqk.spark.core

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by 428900 on 2017/5/2.
  */
object RddBasedOnCollections {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("MyApp")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = 1 to 100

    val rdd = sc.parallelize(numbers)

    val sum = rdd.reduce( _ + _)

    println ("求和（1 + 2 + ... + 99 + 100 ＝ ）" + sum)

    sc.stop()

  }
}
