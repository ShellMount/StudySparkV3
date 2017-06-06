package com.imqk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 在线热搜词汇总，统计前30分钟的数据 并对数据每5分钟更新一次
  * 能不能用之前的 updateByKey ? 呵呵
  * Created by 428900 on 2017/5/14.
  *
  * 技术实现： SPARK STREAMING 提供了滑动窗口支持，可以使用该场
  * 使用 reduceByKeyAndWindow 来实现具体操作
  */

object OnLineHosttestItems {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("OnLineHosttestItems")
    conf.setMaster("local[2]")

    //val ssc = new StreamingContext(conf, Durations.seconds(3))
    // 设置 Batch Interval 是在SPARK STREAMING 中生成基本JOB的时间单位
    // 窗口宽度时间长度一定是该 值的整数倍
    val ssc = new StreamingContext(conf, Seconds(10))

    // hottestDStream2 函数需要使用 checkPoint
    ssc.checkpoint("F:\\STUDY\\TempDir\\CheckPoint")

    val hottestStream = ssc.socketTextStream("master", 9999)

    // 用户搜索数据格式 ： (word, item)
    // 将数据转为 (item, 1)
    val searchPair = hottestStream.map(_.split(" ")(1)).map(item => (item, 1))

    // 窗口宽度60， 每20秒滑动一次
    val hottestDStream = searchPair.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60), Seconds(20))

    // 更高效的处理方式：上面同样的功能，滑动计数:
    // v1 - v2 --> 是指滑前后存在重叠部分的数据，进行复用，不用重复计算，逻辑有些复杂哈哈哈。
    val hottestDStream2 = searchPair.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2,
                          (v1: Int, v2: Int) => v1 - v2, Seconds(60), Seconds(20) )

    val hottest = hottestDStream.transform(hotestItemRDD => {
      val top3 = hotestItemRDD.map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1)).take(3)

      top3.foreach(println)

      hotestItemRDD // 满足格式的返回值，在这里不重要，我们只看上面的 println 内容即可
    })

    hottest.print()

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
