package com.imqk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ShellMount
  * 取得TopN
  */
object TopNBasic {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("TopN排序")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines = sc.textFile("F:\\STUDY\\hadoop\\DOCUMENT\\TopN_basic.txt")

    val pairs = lines.map(line => (line.toInt, line))

    val sortedPairs = pairs.sortByKey(false)

    val sortedData = sortedPairs.map(pair => pair._2)

    val n = 5
    val topN = sortedData.take(n)

    topN.foreach(println)
  }
}
