package com.imqk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 428900 on 2017/5/14.
  */
object OnLineBlackListFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountOnLine")
    conf.setMaster("local[2]")

    //val ssc = new StreamingContext(conf, Durations.seconds(3))
    val ssc = new StreamingContext(conf, Seconds(10))

    // 黑名单数据
    val blackList = Array(("hadoop", true), ("Scala", true), ("Spark", true))
    val blackListRDD = ssc.sparkContext.parallelize(blackList, 8)

    val sdsClickStream = ssc.socketTextStream("master", 9999)

    // 广告点击数据格式： (time,name)
    // 返回结果 (name, (time, name))
    val sdsClickStreamFormated = sdsClickStream.map{ads => (ads.split(" ")(1), ads) }

    val validClickDStream = sdsClickStreamFormated.transform(userClickRDD => {
      // leftOuterJoin: join后的结果，左边的KEY全部存在，忽略右边的
      val joinedBlackListRDD = userClickRDD.leftOuterJoin(blackListRDD)

      // joinedItem: = (name, ((time, name), boolean))
      // boolean = true 的情况，需要过滤
      // 下面的判断逻辑为： 只在 字段值存在，即为黑名单中的内容
      val validClicked =  joinedBlackListRDD.filter(joinedItem => {
        println (s"joinedBlackListRDD中每一条的值：= $joinedItem")
        // 在黑名单中的，返回TRUE，这个TRUE 将在filter 因满足条件而被过滤掉，即：剩下的都是OK的
        if (joinedItem._2._2.getOrElse(false)) false
        else true
      })

      validClicked.map(validClick => {validClick._2._1})
    })

    println("剩下的都是有效的点击：")
    validClickDStream.print()

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
