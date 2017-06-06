package com.imqk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ShellMount
  * 取得分组数据中的各自TopN
  */
object TopNGroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("TopNGroup排序")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines = sc.textFile("F:\\STUDY\\hadoop\\DOCUMENT\\TopN_group.txt")

    val pairs = lines.map(line => (line.split(",")(0), line.split(",")(1).trim()))

    val grouped = pairs.groupByKey()

    val sorted = grouped.sortByKey(true).map(x => (x._1, x._2.toList.sortWith(_.toInt > _.toInt)))

    sorted.collect.foreach(println)

  }
}
