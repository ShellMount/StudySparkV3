package com.imqk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

/**
  * 这是一个比较综合的例子
  * 使用 SPARK STREAMING + SPARK SQL 在线计算不同类别中最热门的商品排名
  *
  * SPARK STREAMING 能使用ML等功能，主要原因是它能使用 foreachRDD , transform
  * 这些接口基于RDD进行操作，所以以RDD为基础进行调用
  *
  * 下文提到的数据格式： user, item, category
  *
  * Created by 428900 on 2017/5/14.
  */
object OnlineTheTop3Item2DB {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("OnlineTheTop3Item2DB")
    conf.setMaster("local[2]")

    val ssc = new StreamingContext(conf, Durations.seconds(10))
    ssc.checkpoint("hdfs://hdmaster:9000/checkPoing")

    val userClickLogsDStrem = ssc.socketTextStream("master", 9999)

    // user, item, category
    val formattedUserClickLogsDStrem = userClickLogsDStrem.map{line => (line.split(" ")(2) + "_" + line.split(" ")(1), 1)}

    val categoryUserClickLogsDStream = formattedUserClickLogsDStrem.reduceByKeyAndWindow(
      //(v1: Int, v2: Int) => v1 + v2,
      //(v1: Int, v2: Int) => v1 -v2,
      _ + _,
      _ - _,
      Seconds(60),
      Seconds(20)
    )

    /**
      * 这里的层级关系可以看出数据结构：
      *   DSTREAM > RDD > PARATITION > RECORD
      */
    categoryUserClickLogsDStream.foreachRDD{
      rdd => {
        // 整理RDD中的数据
        val categoryItemRow = rdd.map{
          reducedItem => {
            val category =  reducedItem._1.split("_")(0)
            val item =  reducedItem._1.split("_")(1)
            val click_count = reducedItem._2
            Row(category, item, click_count)
          }
        }

        // 转换 SQL HIVE 表
        val structType = StructType(Array(
          StructField("category", StringType, true),
          StructField("item", StringType, true),
          StructField("click_count", IntegerType, true)
        ))

        val hiveContext = new HiveContext(rdd.context)
        val categoryItemDF = hiveContext.createDataFrame(categoryItemRow, structType)

        categoryItemDF.registerTempTable("categoryItem")

        // HIVE中取得数据
        val sqlText = "SELECT category, item, click_count FROM " +
          "(SELECT category, item, click_count, row_number() " +
          "   OVER (PARTITION BY category ORDER BY click_count DESC) " +
          "   RANK FROM categoryItem) SUBQUERY " +
          "WHERE rank <= 3"
        val resultDF = hiveContext.sql(sqlText)

        resultDF.show()

        // 所有的格式都要转为RDD进行处理，
        // 别外，下面的操作，是否可以弄功能外去再操作， xxx.map(...)
        val resultRDD = resultDF.rdd

        // 存储进入数据库
        // 以前试图将数据存储进入HIVE未成功
        resultRDD.foreachPartition{
          recordsPartition => {
            if (recordsPartition.nonEmpty){ // 这个判断应该增加始于 foreachRDD, 在这里操作，会有重复数据（每10秒执行一次）
              println ("准备插入数据")
              val connection = ConnectionPool.getConnection()
              println ("获取数据库连接成功")

              recordsPartition.foreach{
                record => {
                  // record 是上面 Row 结构，因此可以 getAs
                  // 特别注意下面的数据类型，但在上面的封装时，明明已经指定了数据类型，也就是说 HIVE 的表中，并没有数据类型
                  val category = record.getAs("category").toString
                  val item =  record.getAs("item").toString
                  val click_count = record.getAs("click_count").toString.toInt
                  val sqlText = s"INSERT INTO CATEGORYTOP3(category, item, click_count) values ('$category', '$item', $click_count)"
                  val statement = connection.createStatement()
                  statement.executeUpdate(sqlText)
                  println ("数据库写入完成")
                }
              }
              ConnectionPool.returnConnection(connection)
            }
          }
        }
      }
    }

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
