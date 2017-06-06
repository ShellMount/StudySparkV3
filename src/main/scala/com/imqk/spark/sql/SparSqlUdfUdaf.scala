package com.imqk.spark.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用户自定义函数在处理处理中的使用
  * UDF 相当于 SPARK SQL 内置函数
  * UDAF 自定义的聚合函数
  *
  * UDF 会被 Spark SQL 中的 Catalyst 封闭成为 Expression 最终通过 eval 方法来计算输入的数据 Row (不是DF中的ROW)
  *
  * Created by 428900 on 2017/5/12.
  */
object SparSqlUdfUdaf {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("SparSqlUdfUdaf")
    conf.setMaster("local")

    val sc =  new SparkContext(conf)
    // cluster
    // val sqlContext = new HiveContext(sc)
    // local
    val sqlContext = new SQLContext(sc)

    // 造数据
    val bigData = Array(
      "Spark",
      "Spark",
      "Hadoop",
      "Hadoop",
      "Spark",
      "Hadoop",
      "Spark",
      "Hadoop"
    )

    // 创建 DF
    val bigDataRDD = sc.parallelize(bigData)
    val bigDataRddRow = bigDataRDD.map(item => Row(item))
    // 构造类型
    val structType = StructType(Array(StructField("word", StringType, true)))
    val bigDataDF = sqlContext.createDataFrame(bigDataRddRow, structType)

    // 注册成临时表
    bigDataDF.registerTempTable("bigDataTable")

    /**
      * 创建自定义函数 UDF，相当于 MYSQL 中的存储过程或函数
      */
    sqlContext.udf.register("computerLength", (input: String) => input.length)

    sqlContext.sql("select word, computerLength(word) from bigDataTable").show()

    /**
      * 下面是 UDAF
      * 与UDF 不同的是，UDAF会对数据进行聚合，相当于一个自定义的聚合功能
      * 它与窗口函数更接近
      */
    sqlContext.udf.register("wordCount", new MyUdaf)

    println("==" * 30 + "下面是UDAF")
    sqlContext.sql("SELECT word, wordCount(word) as count, computerLength(word) as length " +
      "FROM bigDataTable GROUP BY word").show()

    // 方便网页查看
    // while(true)()
  }
}

/**
  * 按照模版实现UDAF
  */
class MyUdaf extends UserDefinedAggregateFunction{

  // 自动导入未实现方法: Ctrl-i
  // 有时候好像是 Alt-Enter, 也能自动导入未实现的方法

  /**
    * 指定输入数据的类型
    * @return
    */
  override def inputSchema: StructType = StructType(Array(StructField("input", StringType, true))) //此处的列名，与使用中的列名，不相关

  /**
    * 聚合操作时所要处理数据的结果的类型
    * @return
    */
  override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType, true)))

  /**
    * 指定UDAF函数计算后返回的结果类型
    * @return
    */
  override def dataType: DataType = IntegerType

  /**
    * 确保一至性
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 在 Aggregate 之前每组数据初始化的结果
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {buffer(0) = 0}

  /**
    * 聚合的时候，有新值进入时，对分组后的聚合数据如何计算
    * 本地的聚合操作，相当于 Hadoop Map-Reduce 中的 Combiner
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  /**
    * 在分布式结果进行 Local Reduce 完成后，需要进行全局级别的Merge 操作
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = { //此处的 Row  不是 RDD、DF中的 Row
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  /**
    * 返回UDAF最后的计算结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
}