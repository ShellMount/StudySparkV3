package com.imqk.spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 428900 on 2017/5/12.
  */
object SparkSqlWindowFunction {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkSqlWindowFunction")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    // 集群使用
    val hiveContext = new HiveContext(sc)
    // 本地使用
    // val hiveContext = new SQLContext(sc)

    hiveContext.sql("use test_for_hive")
    hiveContext.sql("DROP TABLE IF EXISTS scores")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS scores(name STRING, score INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'") //默认值与本行同
    hiveContext.sql("LOAD DATA LOCAL INPATH '/home/manager/Document/TopN_group.txt' INTO TABLE scores")

    // 使用开窗函数：分组且排名
    /**
      * 使用子查询的方式完成目标数据的提取，并在目标数据内部使用窗口函数 row_number 分组排序
      * PARTITION BY： 指定窗口函数分组的KEY
      * ORDER BY： 分组后排序
       */
    val result = hiveContext.sql(
      "SELECT name, score FROM ( " +
        "SELECT " +
        "name," +
        "score," +
        "row_number() OVER (PARTITION BY name ORDER BY score DESC) rank" +
        " FROM scores " +
        ") sub_scores " +
        "WHERE rank <= 4"
    )

    result.show()

    // 保存进入数据中
    hiveContext.sql("DROP TABLE IF EXISTS sortedResultScores")

    // 下面一行不成功:新版本不支持？
    // result.saveAsTable(sortedResultScores)
  }

}
