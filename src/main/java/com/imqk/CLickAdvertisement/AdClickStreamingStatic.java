package com.imqk.CLickAdvertisement;

import kafka.serializer.StringDecoder;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.sql.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by 428900 on 2017/5/14.
 * 广告点击时，从KAFKA收到的数据格式：
 * timestamp, ip, userId, adId, province, city
 */
public class AdClickStreamingStatic {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("AdClickStreamingStatic");
        // Executor Core 最佳分配为奇数个，3，5，7
        conf.setMaster("local[4]");
        //conf.setMaster("spark://sparkmaster:7077");

        // 启用多个 StreamingContext 时，需要将前面的关闭，即同一时刻只有一个 StreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(15));

        /*
            数据来源，可以是 file, 网络，HDFS，FLUME，KAFKA，SOCKET
            监听时间段没有数据，也会跑一个JOB，造成资源浪费
         */
        Map<String, String> kafkaParameters = new HashMap<String, String>();
        kafkaParameters.put("metadata.broker.list", "master:9092,worker-1:9092,worker-2:9092");

        Set<String> topics = new HashSet<String>();
        topics.add("AdClick");

        JavaPairInputDStream<String, String> adCLickedStreaming = KafkaUtils.createDirectStream(ssc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParameters,
                topics);

        // 后面的操作，就像处理 RDD。事实上是 DStream, DStream 是RDD 的父类、模板
        // ITEM: timestamp, ip, userId, adId, province, city
        // KAFKA 输入的数据本身是 Tuple2<String,String>
        JavaPairDStream<String, Long> pairs = adCLickedStreaming.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                String[] splited = t._2.split("\t");
                String clickedRecord = StringUtils.join(splited, "_");
                return new Tuple2<String, Long>(clickedRecord, 1L);
            }
        });

        // new Function2<Long, Long, Long> : 此处三个参数为：前一条记录的VALUE，后一条记录的VALUE，返回VALUE
        // JavaPairDStream<String, Long> ? 这两个参数是怎么回事？ 最后的数据结果内部是<String, Long>
        // 还是说这是继承 pairs 的结构？
        // 计算每个用户的点击量：
        JavaPairDStream<String, Long> adClickedUser = pairs.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        /**
         * 计算有效点击
         * 通常使用机器学习来判断
         * 简单一点，可以通过 batch Duration 中点击次数来判断是否是恶意点击
         * 单位时间内，同一IP访问量    --> 列入黑名单
         * 同一用户一天内总点击量      --> 列入黑名单
         *
         * 黑名单:动态生成，存储在DB中
         */
        // 有效性过滤
        JavaPairDStream<String, Long> filteredAdClickedInBatch = adClickedUser.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> v1) throws Exception {
                if (1 < v1._2()) {
                    return false;
                } else {
                    return true;
                }
            }
        });

        // 下面代码有问题
        // 存储数据条目
        /*filteredAdClickedInBatch.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
                        *//**
                         * 使用数据库连接（MYSQL）
                         * 传入的参数是 Iterator 类型的集合，所以在这里使用批量处理
                         * 保存 userId, adId, clickCount, timstamp
                         *//*
                    }
                });
                return null;
            }
        });*/


        // 黑名单过滤
        JavaPairDStream<String, Long> blackListBasedOnHistory = filteredAdClickedInBatch.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> v1) throws Exception {
                // timestamp, ip, userId, adId, province, city
                String[] splited = v1._1().split("\t");
                String date = splited[0];
                String userId = splited[2];
                String adId = splited[3];

                // 根据上面的信息，查询数据库，获取总的点击次数
                // 一天内 > 50 次 则确定是否在黑名单表中
                int clickedCountTotalToday = 80;

                if (clickedCountTotalToday > 50) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        /**
         * 对黑名单的整个RDD去重
         */
        JavaDStream<String> userIdBlackListBasedOnHistory = blackListBasedOnHistory.map(new Function<Tuple2<String,Long>, String>() {
            @Override
            public String call(Tuple2<String, Long> v1) throws Exception {
                return v1._1().split("\t")[2];
            }
        });

        JavaDStream<String> uniqUserIdBlackListBasedOnHistory = userIdBlackListBasedOnHistory.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                return rdd.distinct();
            }
        });

        // 将黑名单写入数据表存储
        // 下面的代码报异常，原因不明
        /*uniqUserIdBlackListBasedOnHistory.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> t) throws Exception {
                        // 存储数据
                        // 包含 UserId 信息的记录
                        // 插入黑名单数据表
                    }
                });
                return null;
            }
        });*/




        filteredAdClickedInBatch.print();
        System.out.println("=======================");
        System.out.println(filteredAdClickedInBatch);

        // 启动框架
        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ssc.close();
    }
}


class JDBCWrapper{
    private static JDBCWrapper jdbcInstance =null;
    private static LinkedBlockingQueue<Connection> dbConnetionPool =new LinkedBlockingQueue<Connection>();
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static JDBCWrapper getJDBCInstance() throws InterruptedException{
        if (jdbcInstance ==null){
            synchronized(JDBCWrapper.class){ //只有一个线程进去
                if (jdbcInstance ==null){ //第二个 线程发现不为空，返回了
                    jdbcInstance = new JDBCWrapper();
                }
            }
        }
        return jdbcInstance;
    }

    private JDBCWrapper() throws InterruptedException{
        for (int i =0 ; i <10 ; i++) {
            //一次建立10个连接
            try {
                Connection conn = DriverManager.getConnection("jdbc:mysql://Master:3306/sparkstreaming","root","root");
                dbConnetionPool.put(conn);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public synchronized Connection  getConnetion (){
        //有一种情况，池子里面没有东西
        while ( 0 == dbConnetionPool.size()){
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } //等待20秒
        }
        return  dbConnetionPool.poll();
    }

    //通用操作 查询、插入、删除；spark是一批一批的数据的批处理
    public int[] doBatch(String sqlText, List<Object[]> paramsList, ExecuteCallBack callback) {
        Connection conn = getConnetion();
        int[] result =null;
        PreparedStatement  preparedStatement =null;
        try {
            conn.setAutoCommit(false);
            preparedStatement = conn.prepareStatement(sqlText);
            for (Object[] parameters: paramsList ){
                for (int i = 0 ;i <parameters.length; i++) {
                    preparedStatement.setObject(i+1, parameters[i]);
                }
                preparedStatement.addBatch();
            }
            result = preparedStatement.executeBatch();

            //callback.resultCallBack((int[]) result);
            conn.commit();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            if (preparedStatement != null ){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            if (conn !=  null ){
                try {
                    dbConnetionPool.put(conn);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        return result;
    }




    public void	doQuery(String sqlText,List<Object[]> paramsList,ExecuteCallBack callback) {

        Connection conn = getConnetion();
        ResultSet result =null;
        PreparedStatement preparedStatement =null;
        try {
            conn.setAutoCommit(false);
            preparedStatement=conn.prepareStatement(sqlText);
            for (Object[] parameters: paramsList ){
                for (int i = 0 ;i <parameters.length; i++) {
                    preparedStatement.setObject(i+1, parameters[i]);
                }

            }
            result =preparedStatement.executeQuery();

            try {
                callback.resultCallBack(result);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            if (preparedStatement != null ){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            if (conn !=  null ){
                try {
                    dbConnetionPool.put(conn);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }


    //弄个回调函数
    interface ExecuteCallBack {
        void resultCallBack(ResultSet result) throws Exception;
    }
}



