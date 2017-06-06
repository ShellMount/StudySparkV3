package com.imqk.spark.stream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 在线热搜词汇总，统计前30分钟的数据 并对数据每5分钟更新一次
  * 能不能用之前的 updateByKey ? 呵呵
  * Created by 428900 on 2017/5/14.
  *
  * 技术实现： SPARK STREAMING 提供了滑动窗口支持，可以使用该场
  * 使用 reduceByKeyAndWindow 来实现具体操作
  */

case class MessageItem(name: String, age: Int)

object SparkStreamingFromKafkaToHive {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("需要添加参数 ： kafkaParmas， topics")
      System.exit(1)
    }

    val Array(brokers, topicList) = args
    val kafkaParmas = Map[String, String]("metadata.broker.list" -> brokers)
    val topics = topicList.split(",").toSet


    val conf = new SparkConf()
    conf.setAppName("OnLineHosttestItems")
    conf.setMaster("local[2]")

    //val ssc = new StreamingContext(conf, Durations.seconds(3))
    // 设置 Batch Interval 是在SPARK STREAMING 中生成基本JOB的时间单位
    // 窗口宽度时间长度一定是该 值的整数倍
    val ssc = new StreamingContext(conf, Seconds(10))

    //val hottestStream = ssc.socketTextStream("master", 9999)
    // 指定泛型，以便后面的 split 能操作， 其实，还是不特别明白什么加 泛型，什么时候不加
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParmas,
      topics).map(_._2.split(",")).foreachRDD(rdd => {
      val hiveContest = new HiveContext(rdd.sparkContext) // rdd的 sparkContext

      import hiveContest.implicits._
      rdd.map(record => MessageItem(record(0).trim, record(1).trim.toInt)).toDF.registerTempTable("temp")

      hiveContest.sql("SELECT COUNT(*) from temp").show()
    })

    /**
      * 1,  把数据写入HIVE
      * 2， 通过JAVA 访问HIVE中的数据
      * 3， 通过FLUME 做原始数据的收集
      * 4， FLUME 将数据传给 KAFKA
      */

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
