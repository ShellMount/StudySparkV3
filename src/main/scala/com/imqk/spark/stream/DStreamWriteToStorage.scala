package com.imqk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by 428900 on 2017/5/14.
  */
object DStreamWriteToStorage {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DStreamWriteToStorage")
    conf.setMaster("local[2]")

    val ssc = new StreamingContext(conf, Durations.seconds(3))

    val lines = ssc.socketTextStream("master", 9999)

    val words = lines.flatMap{line => line.split(" ")}

    val pairs = words.map{word => (word, 1)}

    val wordsCount = pairs.reduceByKey(_ + _)

    wordsCount.print()

    /**
      * 这里的层级关系可以看出数据结构：
      *   DSTREAM > RDD > PARATITION > RECORD
       */
    wordsCount.foreachRDD{rdd => rdd.foreachPartition{
        recordsPartition => {
          val connection = ConnectionPool.getConnection
          recordsPartition.foreach{
            record => {
              val sqlText = "INSERT INTO STREAMING_ITEMCOUNT(item, count) values ('$record._1', '$record._2')"
              val statument = connection.createStatement()
              statument.executeUpdate(sqlText)
            }
          }
          ConnectionPool.returnConnection(connection)
        }
      }
    }

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
