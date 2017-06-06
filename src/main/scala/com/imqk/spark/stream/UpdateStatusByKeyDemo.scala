package com.imqk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by 428900 on 2017/5/14.
  */
object UpdateStatusByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountOnLine")
    conf.setMaster("local[2]")

    val ssc = new StreamingContext(conf, Durations.seconds(3))
    ssc.checkpoint("F:\\STUDY\\TempDir\\CheckPoint")

    val lines = ssc.socketTextStream("master", 9999)

    val words = lines.flatMap{line => line.split(" ")}

    val pairs = words.map{word => (word, 1)}

    // val wordsCount = pairs.reduceByKey(_ + _)
    // 当函数体中的数据结构是复合体，MAP，SET，ARRAY，SEQ，OPTION，等时，需要在调用的时候，明确结构
    // 这里的 (oldValueSeq: Seq[Int], newValueOption: Option[Int]) 并不能用 item 来替代，
    // 然后在后面的函数体中处理： item(0), item(1), 因为它并不是 map 函数中的调用
    // 下面的 wordsCount 是累加历史出现的词统计
    val wordsCount = pairs.updateStateByKey((oldValueSeq: Seq[Int], newValueOption: Option[Int]) => {
      var newValue = newValueOption.getOrElse(0)
      for (value <- oldValueSeq){
        println("一个 SEQ 只有一个值！")
        newValue += value
      }
      Some(newValue)
    })

    wordsCount.print()

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
