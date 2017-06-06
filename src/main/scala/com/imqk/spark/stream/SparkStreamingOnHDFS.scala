package com.imqk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by 428900 on 2017/5/14.
  */
object SparkStreamingOnHDFS {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("WordCountOnLine")
    conf.setMaster("local[2]")

    // val ssc = new StreamingContext(conf, Durations.seconds(3))

    val checkPointDir = "hdfs://hdmaster:9000/checkPoint"

    // 必须写在这里
    def newContext(): StreamingContext ={
      createStreamingContext(conf, checkPointDir)
    }

    // 此处的 newContext 位置是不能带参数的
    val ssc = StreamingContext.getOrCreate(checkPointDir, newContext)

    //val lines = ssc.socketTextStream("master", 9999)
    val lines = ssc.textFileStream("hdfs://hdmaster:9000/streaming")


    val words = lines.flatMap{line => line.split(" ")}

    val pairs = words.map{word => (word, 1)}

    val wordsCount = pairs.reduceByKey(_ + _)

    wordsCount.print()

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()

  }

  def createStreamingContext(conf: SparkConf, checkPointDir: String): StreamingContext = {
    println("正在创建一个新的 StreamingContext... To HDFS : " + checkPointDir)
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.checkpoint(checkPointDir)
    ssc
  }


}
