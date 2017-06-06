package com.imqk.spark.stream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by 428900 on 2017/5/14.
  */
object SparkStreamOnKafkaDirect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkStreamOnKafkaDirect")
    conf.setMaster("local[3]")

    val ssc = new StreamingContext(conf, Durations.seconds(3))

    // val lines = ssc.socketTextStream("master", 9999)
    val kafkaParameters= Map("metadata.broker.list" -> "master:9092,worker-1:9092,worker-2:9092")

    val topics = Set("testMyFirstKafkaMessage")

    // 这里的 createDirectStream 需要指定类型，为啥有时候又不用指定呢？
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
                kafkaParameters,
                topics)

    val words = lines.flatMap{line => line._2.split(" ")}

    val pairs = words.map{word => (word, 1)}

    val wordsCount = pairs.reduceByKey(_ + _)

    wordsCount.print()

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
