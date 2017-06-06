package com.imqk.spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 428900 on 2017/5/11.
  */
object SparkSql2Hive {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("SparkSql2Hive")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    // 使用 HiveContext
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use test_for_hive")
    hiveContext.sql("DROP TABLE IF EXISTS people")
    //hiveContext.sql("CREATE TABLE IF NOT EXISTS people (name STRING, age INT)")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS people (name STRING, age INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/home/manager/Document/people.txt' INTO TABLE people")


    hiveContext.sql("DROP TABLE IF EXISTS peopleScore")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS peopleScore (name STRING, score INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    hiveContext.sql("LOAD DATA LOCAL INPATH '/home/manager/Document/peopleScores.txt' INTO TABLE peopleScore")

    // 使用通过 HIVECONTEXT 生成的表,获取数据
    val resultDF = hiveContext.sql("SELECT pi.name, pi.age, ps.score " +
      "FROM people pi JOIN peopleScore ps ON pi.name=ps.name " +
      "WHERE ps.score > 20")

    resultDF.show()

    // 使用全表
    val peopleScoreDF = hiveContext.table("peopleScore")
    peopleScoreDF.show()

    // 将数据存入一个新表：但是 DataFrame 没有 saveAsTable 方法了，咋办？
    // 这里数据的写入，方面有问题。

  }
}
