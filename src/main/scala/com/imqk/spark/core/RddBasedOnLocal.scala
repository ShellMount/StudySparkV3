package com.imqk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 428900 on 2017/5/2.
  */
object RddBasedOnLocal {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("MyApp")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = 1 to 100

    val rdd = sc.textFile("F:\\STUDY\\hadoop\\DOCUMENT\\IF0000_1min.txt")

    val linesLength = rdd.map(_.length)

    val sum = linesLength.reduce(_ + _)

    println ("文件总长度 ： " + sum)

    sc.stop()

  }
}
