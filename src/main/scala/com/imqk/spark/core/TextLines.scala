package com.imqk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 428900 on 2017/5/2.
  */
object TextLines {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("TextLines")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = 1 to 100

    val lines = sc.textFile("F:\\STUDY\\hadoop\\DOCUMENT\\IF0000_1min.txt")

    val lineCount = lines.map( line => (line, 1))

    val textLines = lineCount.reduceByKey( _ + _, 1)

    textLines.collect.foreach(pair => println (pair._1 + "--->" + pair._2))

    sc.stop()

  }
}
