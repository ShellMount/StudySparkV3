package com.imqk.spark.sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 428900 on 2017/5/9.
  */

object SparkSqlWithJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkSqlWithJoin")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // 导入 sqlContext 的隐式转换

    //val lines = sc.textFile("F:\\STUDY\\hadoop\\DOCUMENT\\person2.json")
    // JSON 应该使用下面的读取方法，直接成为 DF
    val peopleScoreDF = sqlContext.read.json("F:\\STUDY\\hadoop\\DOCUMENT\\people2.json")

    peopleScoreDF.registerTempTable("personScore")

    val execStudentDF = sqlContext.sql("select name, score from personScore where score > 10")

    val execStudentNameList = execStudentDF.rdd.map{row => row(0)}.collect()

    // 构造新JSON : 名字－年龄表
    val peopleInfo = Array(
      "{\"name\":\"Michael\", \"age\": 20}",
      "{\"name\":\"Andy\", \"age\":130}",
      "{\"name\":\"Justin\", \"age\":9000}"
    )

    val peopleInfoRDD = sc.parallelize(peopleInfo)

    val peopleInfoDF = sqlContext.read.json(peopleInfoRDD)

    peopleInfoDF.registerTempTable("studentAge")

    var sqlText = "select name, age from studentAge where name in ("

    for (i <- 0 until(execStudentNameList.length)){
      sqlText += "'" + execStudentNameList(i) + "'"
      //sqlText += ("'%s'", execStudentNameList(i))
      if (i < execStudentNameList.length -1){
        sqlText += ","
      }
    }

    sqlText += ")"

    println(sqlText)
    val execNameAgeDF = sqlContext.sql(sqlText)

    // JOIN 准备
    // 这里 join 的操作是否太哆嗦，是否有更简便的方法转换下面的RDD？为何这里又必须使用 collect 方法
    val execNameScoreRDD = sc.parallelize(execStudentDF.collect().map(row => (row.get(0), row.get(1))))
    val execNameAgeRDD = sc.parallelize(execNameAgeDF.collect().map(row => (row(0), row(1))))

    // JOIN : (Michael,(90,20))
    val allInfoJoinRDD = execNameScoreRDD.join(execNameAgeRDD)

    // 展开： Row(Michael, 90, 20)
    // 下面两行的结果，看起来一样，但collect()处理后，会失去很多属性，在后文DF中，会有问题
    // 它们之间有什么微妙的关系？
    //val allInfoRDD = allInfoJoinRDD.collect().map{item => Row(item._1, item._2._1, item._2._2)}
    val allInfoRDD = allInfoJoinRDD.map{item => Row(item._1, item._2._1, item._2._2)}

    allInfoRDD.foreach(println)

    val schemaString = Array("name", "score", "age")
    val schema = StructType(schemaString.map{fieldName => StructField(fieldName, StringType, true)})

    // 转为 DF, 为啥不成功？
    val peopleDF = sqlContext.createDataFrame(allInfoRDD, schema)

    // 使用 DF
    peopleInfoDF.registerTempTable("peopleInfo")

    // 保存新生成的DF数据，也可以使用 parquet 保存数据
    peopleInfoDF.write.format("json").save("F:\\STUDY\\OutPutApp\\sqpkSqlWithJoin.json")

    peopleInfoDF.show()

  }
}
