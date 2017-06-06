package com.imqk.spark.sql

import java.sql.{DriverManager, ResultSet, SQLException}

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SPARK SQL 读取数据库
  * Created by 428900 on 2017/5/11.
  */
object SparkSqlJdbc2MySql {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("SparkSqlJdbc2MySql")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val reader = sqlContext.read.format("jdbc")

    // SPARK SQL 读取数据
    reader.option("url", "jdbc:mysql://master:3306/future")
    //此外采用XSHELL TUNNEL映射的方式连接远程数据库
    reader.option("url", "jdbc:mysql://localhost:3306/future")

    reader.option("driver", "com.mysql.jdbc.Driver")
    reader.option("user", "root")
    reader.option("password", "manager!74")
    reader.option("dbtable", "if0000")

    val mysqlDataDF_IF0000 = reader.load()


    mysqlDataDF_IF0000.show()

    // 对数据进行处理
    // ...

    // 不执行后续写入操作
    sys.exit(0)

    // 写入数据库：一行一行地写入，效率低下
    // mysqlDataDF_IF0000.foreach(row => MysqlData().writeIntoMysql(row))

    // 写入数据库：一块一块地写入，OK
    mysqlDataDF_IF0000.foreachPartition(items => MysqlData.apply().saveIntoMysql(items))

  }
}

case class MysqlData() {

  /**
    * SCALA 操作数据，主要是保存数据
    */

  def queryFromMysql() = {
     // Change to Your Database Config
     val conn_str = "jdbc:mysql://localhost:3306/future?user=root&password=manager!74"

     // Load the driver
     //classOf[com.mysql.jdbc.Driver]

     // Setup the connection
     val conn = DriverManager.getConnection(conn_str)

     try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

        // Execute Query
        val rs = statement.executeQuery("SELECT quote FROM quotes LIMIT 5")

        // Iterate Over ResultSet
        while (rs.next) {

          println(rs.getString("quote"))

        }
     } catch {

        case e: SQLException =>
             e.printStackTrace()

     } finally {

        if (conn != null) conn.close()

     }
  }

  def saveIntoMysql(items: Iterator[Row]) ={

    // create database connection
    val dbc = "jdbc:mysql://localhost:3306/future?user=root&password=manager!74"

    //classOf[com.mysql.jdbc.Driver]

    val conn = DriverManager.getConnection(dbc)

    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

    // do database insert : items
    try {
       val prep = conn.prepareStatement("INSERT INTO quotes (quote, author) VALUES (?, ?) ")

       prep.setString(1, "Nothing great was ever achieved without enthusiasm.")

       prep.setString(2, "Ralph Waldo Emerson")

       prep.executeUpdate
    } finally {

       conn.close

    }
  }
}