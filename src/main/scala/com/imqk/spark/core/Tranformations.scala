package com.imqk.spark.core

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by 428900 on 2017/5/2.
  */
object Tranformations {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Tranformation")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val nums = sc.parallelize(1 to 10)

    val mapped = nums.map(item => item * 2)

    val filtered = mapped.filter(item => item > 5)

    //filtered.collect.foreach(println)

    val bigData = Array("Scala One", "Spark Two", "Java Three", "Hadoop Four", "Tachyon Five")
    val bigDataString = sc.parallelize(bigData)
    val words = bigDataString.flatMap(line => line.split(" "))

    words.collect.foreach(println)


    ////////////
    groupByKeyTranformation(sc)

    ////
    reduceByKeyTranformation(sc)

    ///
    joinTranformation(sc)

    //
    cogroupTranformation(sc)

    sc.stop()

  }


  def groupByKeyTranformation(sc: SparkContext) = {
    val data =  Array(Tuple2(100, "Spark"), Tuple2(100, "Hadoop"), Tuple2(80, "Scala"))
    val dataRdd = sc.parallelize(data)
    val grouped = dataRdd.groupByKey()

    grouped.collect.foreach(println)
  }


  def reduceByKeyTranformation(sc: SparkContext) = {
    // wordCounter 的案例
    None
  }

  def joinTranformation(sc: SparkContext) = {
    val studentNames = Array(
      Tuple2(1, "Spark"),
      Tuple2(2, "Scala"),
      Tuple2(3, "Hadoop"),
      Tuple2(4, "Tachyan")
    )

    val studentSores = Array(
      Tuple2(1, 100),
      Tuple2(2, 110),
      Tuple2(3, 200),
      Tuple2(4, 50)
    )

    val names = sc.parallelize(studentNames)
    val scores = sc.parallelize(studentSores)
    val studentNameAndScore = names.join(scores)
    studentNameAndScore.collect.foreach(println)
  }


  def cogroupTranformation(sc: SparkContext) = {
    val nameList = Array(
      Tuple2(1, "Spark"),
      Tuple2(2, "Scala"),
      Tuple2(3, "Hadoop"),
      Tuple2(4, "Tachyan")
    )

    val scoreList = Array(
      Tuple2(1, 100),
      Tuple2(2, 110),
      Tuple2(3, 200),
      Tuple2(4, 50),
      Tuple2(1, 100),
      Tuple2(2, 110),
      Tuple2(3, 200),
      Tuple2(4, 50)
    )

    val names = sc.parallelize(nameList)
    val scores = sc.parallelize(scoreList)

    val cogroup = names.cogroup(scores)
    cogroup.collect.foreach(println)

  }


}
