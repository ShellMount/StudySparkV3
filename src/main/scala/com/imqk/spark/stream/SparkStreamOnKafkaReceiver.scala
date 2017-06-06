package com.imqk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by 428900 on 2017/5/14.
  */
object SparkStreamOnKafkaReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkStreamOnKafkaReceiver")
    conf.setMaster("local[3]")

    val ssc = new StreamingContext(conf, Durations.seconds(3))

    // val lines = ssc.socketTextStream("master", 9999)
    val topicConsumerConcurrency= Map("testMyFirstKafkaMessage" -> 2)

    val lines = KafkaUtils.createStream(ssc,
                                        "master:2181,worker-1:2181,worker-2:2181",
                                        "TEST",
                                        topicConsumerConcurrency)

    val words = lines.flatMap{line => line._2.split(" ")}

    val pairs = words.map{word => (word, 1)}

    val wordsCount = pairs.reduceByKey(_ + _)

    wordsCount.print()

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
