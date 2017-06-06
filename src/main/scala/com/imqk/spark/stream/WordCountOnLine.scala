package com.imqk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by 428900 on 2017/5/14.
  */
object WordCountOnLine {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountOnLine")
    conf.setMaster("local[2]")

    val ssc = new StreamingContext(conf, Durations.seconds(3))

    val lines = ssc.socketTextStream("master", 9999)

    val words = lines.flatMap{line => line.split(" ")}

    val pairs = words.map{word => (word, 1)}

    val wordsCount = pairs.reduceByKey(_ + _)

    wordsCount.print()

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
