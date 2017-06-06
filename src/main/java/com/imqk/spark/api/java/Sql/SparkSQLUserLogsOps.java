package com.imqk.spark.api.java.Sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.hive.HiveContext;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 处理 HIVE 中日志数据
 * 模拟项目案例
 * Created by 428900 on 2017/5/12.
 */
public class SparkSQLUserLogsOps {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkSQLUserLogsOps");
        conf.setMaster("spark://sparkmaster:7077");

        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc.sc());

        String twodaysageo = getTwoDaysAgo();
        pvStatistic(sqlContext, twodaysageo);

        uvStatistic(sqlContext);

        hotChannel(sqlContext, twodaysageo);

        goOutRate();

        // newUserPercent();
    }

    private static void hotChannel(HiveContext sqlContext, String twodaysageo) {
        sqlContext.sql("use test_for_hive");

        // 计算PV
        String sqlPvText = "SELECT date, pageID, pv " +
                "FROM (SELECT date, pageID, COUNT(1) pv FROM userLogs " +
                "WHERE action = 'View'  AND date = '2017-05-11' " +
                "GROUP BY date, pageID) " +
                "SUBQUERY ORDER BY pv DESC";
        Dataset hot = sqlContext.sql(sqlPvText);
        hot.show(10);
    }

    private static void pvStatistic(HiveContext sqlContext, String twodaysageo) {
        sqlContext.sql("use test_for_hive");

        // 计算PV
        String sqlPvText = "SELECT date, channelID, channelpv " +
                "FROM (SELECT date, channelID, COUNT(1) channelpv FROM userLogs " +
                "WHERE action = 'View'  AND date = '2017-05-11' " +
                "GROUP BY date, channelID) " +
                "SUBQUERY ORDER BY channelpv DESC";
        Dataset pv = sqlContext.sql(sqlPvText);
        pv.show(10);
    }

    private static void goOutRate() {
        // 计算跳出率
        //Dataset pvCount = sqlContext.sql("SELECT count(*) from userlogs where action='View' and date='2017-05-11");
        /**
         * // 跳出率计算：BY SCALA
         val totalTargetPV = sqlContext.sql("SELECT count(*) from userlogs where action='View' and date='2017-05-11").collect(0).get(0)
         val targetResult = sqlContext.sql("SELECT count(*) FROM (SELECT count(*) totalNumber from userlogs where action='View' and date='2017-05-11' GROUP BY userID HAVING totalNumber=1) targetTable").collect
         val pv1 = targetResult(0).get(0)
         val percent = pv1.toString.toDouble / totalTargetPV.toString.toDouble
         val percent = BigDecimal.valueOf(pv1.toString.toDouble) / BigDecimal.valueOf(totalTargetPV.toString.toDouble)

         */
    }

    private static void uvStatistic(HiveContext sqlContext) {
        sqlContext.sql("use test_for_hive");

        // 计算UV
        String sqlUvText = "SELECT date, pageID, uv " +
                "    FROM (SELECT date, pageID, COUNT(*) uv " +
                "        FROM (SELECT date, pageID, userID " +
                "            FROM userlogs " +
                "            WHERE date = '2017-05-11'" +
                "            GROUP BY date, pageID, userID)" +
                "            INNERQUERY GROUP BY date, pageID) " +
                "    RESULTQUERY ORDER BY uv DESC   ";
        Dataset uv = sqlContext.sql(sqlUvText);
    }


    private static String getTwoDaysAgo() {
        /**
         * 计算昨天
         */
        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -2);

        Date twodaysageo = cal.getTime();
        return date.format(twodaysageo);
    }
}
