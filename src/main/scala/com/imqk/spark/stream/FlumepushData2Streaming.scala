package com.imqk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by 428900 on 2017/5/14.
  */
object FlumepushData2Streaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("FlumePushData2Streaming")
    conf.setMaster("local[3]")

    val ssc = new StreamingContext(conf, Durations.seconds(3))

    // 在LINUX上使用 nc -lk 9999 产生的模拟端口
    // val lines = ssc.socketTextStream("master", 9999)
    // 创建端口，接受FLUME推送数据过来
    val flumeStream = FlumeUtils.createStream(ssc, "master", 9999)

    //val words = events.flatMap{event => event.event.getBody.array().toString.split(" ")}
    //val words = flumeStream.flatMap{event => event.event.getBody.array().toString.split(" ")}

    val words = flumeStream.flatMap{event => new String(event.event.getBody.array()).split(" ")}

    val pairs = words.map{word => (word, 1)}

    val wordsCount = pairs.reduceByKey(_ + _)

    wordsCount.print()

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
