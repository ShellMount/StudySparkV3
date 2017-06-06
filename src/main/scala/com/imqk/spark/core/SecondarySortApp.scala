package com.imqk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ShellMount
  * 二次排序解决方案
  * 实现第一列值相同的情况，继续使用次列应用于排序
  * extends Ordered[SecondarySortKey] with Serializable
  */

/**
    待排序数据格式（secondSort.txt）：
    1 100
    2 500
    3 300
    4 50
    2 20
    3 30
    4 200
    2000 20
    3 100
  */

class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(other: SecondarySortKey): Int = {
    if (this.first - other.first != 0){
      this.first - other.first
    } else {
      this.second - other.second
    }
  }
}

object SecondarySortApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("两列排序")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("F:\\STUDY\\hadoop\\DOCUMENT\\secondSort.txt")

    val pairWithSortKey = lines.map{line => (newKey(line), line)}

    pairWithSortKey.map(pair => println(pair._1, pair._2))

    val sorted = pairWithSortKey.sortByKey(true)
    val sortedResult = sorted.map(sortedLine => sortedLine._2)

    sortedResult.collect().foreach(println)
  }

  def newKey(line: String): SecondarySortKey = {
    {
      val (first, second) = (line.split(" ")(0).toInt, line.split(" ")(1).toInt)
      val key = new SecondarySortKey(first, second)
      println(key.toString)
      key
    }
  }
}
