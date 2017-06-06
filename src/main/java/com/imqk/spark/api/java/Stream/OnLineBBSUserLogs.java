package com.imqk.spark.api.java.Stream;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;


/**
 * Created by 428900 on 2017/5/14.
 */
public class OnLineBBSUserLogs {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("OnLineBBSUserLogs");
        // Executor Core 最佳分配为奇数个，3，5，7
        conf.setMaster("local[3]");
        //conf.setMaster("spark://sparkmaster:7077");

        // 启用多个 StreamingContext 时，需要将前面的关闭，即同一时刻只有一个 StreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         数据来源，可以是 file, 网络，HDFS，FLUME，KAFKA，SOCKET
         监听时间段没有数据，也会跑一个JOB，造成资源浪费
         */
        Map<String, String> kafkaParameters = new HashMap<String, String>();
        kafkaParameters.put("metadata.broker.list", "master:9092,worker-1:9092,worker-2:9092");
        Set topics = new HashSet<String>();
        topics.add("UserLogs");
        JavaPairDStream lines = KafkaUtils.createDirectStream(
                ssc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParameters,
                topics
        );

        // 在线PV 计算
        onlinePV(lines);

        // 在线 UV 计算
        onlineUV(lines);
        
        // 注册用户计算
        onlineRegistered(lines);

        // 跳出率计算
        onlineJumped(lines);

        // 在线不同模块的 PV
        onlineChannelPv(lines);

        // 还有很多种逻辑的可以计算的内容，就暂时不实现了，有时间将来可以补齐：视频第99 课

        // 启动框架
        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ssc.close();
    }

    private static void onlineChannelPv(JavaPairDStream lines) {

        JavaPairDStream<Long, Long> pairs = lines.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                String[] logs = t._2().split("\t");
                String channelId = logs[4];   // 是字符串 channelId
                return new Tuple2<String, Long>(channelId, 1L);
            }
        });

        // 求和
        JavaPairDStream<Long, Long> pvByChannelCount = pairs.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        // 分版块的PV
        pvByChannelCount.print();
    }

    private static void onlineJumped(JavaPairDStream lines) {
        // 后面的操作，就像处理 RDD。事实上是 DStream, DStream 是RDD 的父类、模板
        JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String[] logs = v1._2.split("\t");
                String action = logs[5].trim();
                if ("View".equals(action)) {
                    return true;
                } else {
                    return false;
                }
            }
        });


        // 取得 USERID：1统计
        JavaPairDStream<Long, Long> pairs = logsDStream.mapToPair(new PairFunction<Tuple2<String, String>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, String> t) throws Exception {
                String[] logs = t._2().split("\t");
                Long userId = Long.valueOf(logs[2] != null ? logs[2] : "-1");
                return new Tuple2<Long, Long>(userId, 1L);
            }
        });

        // 求和
        JavaPairDStream<Long, Long> UVCount = pairs.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        // 访问数据 为1 的，为跳出的用户
        // 后面的操作，就像处理 RDD。事实上是 DStream, DStream 是RDD 的父类、模板
        JavaPairDStream<Long, Long> jumpedUserDStream = UVCount.filter(new Function<Tuple2<Long, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Long, Long> v1) throws Exception {
                if (1 == v1._2()) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        System.out.println("跳出率：");
        jumpedUserDStream.count().print();
    }

    /**
     *
     * @param lines
     */
    private static void onlineRegistered(JavaPairDStream lines) {
        // 后面的操作，就像处理 RDD。事实上是 DStream, DStream 是RDD 的父类、模板
        JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String[] logs = v1._2.split("\t");
                String action = logs[5].trim();
                if ("Register".equals(action)) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        logsDStream.count().print();
    }


    /**
     * UV, 需要做 同一个用户访问的多个页面的去重操作
     * DSTREAM 中没有 distinct 方法
     * 则需要使用 DStream 的 transform ，在该 方法中直接对RDD 进行 distinct 操作
     *
     * @param lines
     */
    private static void onlineUV(JavaPairDStream lines) {
        // 后面的操作，就像处理 RDD。事实上是 DStream, DStream 是RDD 的父类、模板
        JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String[] logs = v1._2.split("\t");
                String action = logs[5].trim();
                if ("View".equals(action)) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        // 格式化用户ID 与 PAGE 关系
        JavaDStream<String> formatedPageUserDStream = logsDStream.map(new Function<Tuple2<String,String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                String[] logs = v1._2.split("\t");
                Long userId = Long.valueOf(logs[2] != null ? logs[2] : "-1");
                Long pageId = Long.valueOf(logs[3]);

                return pageId + "_" + userId;
            }
        });

        // 去重
        JavaDStream<String> distinctPageUserDStream = formatedPageUserDStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
                return v1.distinct();
            }
        });

        // 取得 PAGEID：1统计
        JavaPairDStream<Long, Long> pairs = distinctPageUserDStream.mapToPair(new PairFunction<String, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(String t) throws Exception {
                String[] logs = t.split("_");
                Long pageId = Long.valueOf(logs[0]);
                return new Tuple2<Long, Long>(pageId, 1L);
            }
        });

        // 求和
        JavaPairDStream<Long, Long> UVCount = pairs.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        // 这里的 print 不会发出 Job
        // JOB 是基于 Durations 时间间格来触发
        // 必须要有动作，否则代码不执行
        // print, saveAsTextFile, saveAsHadoopFiles,foreachRDD
        // 最重要的是 foreachRDD,因为结果，一般会存储在 Redis, DB,DashBoard
        UVCount.print();
        System.out.println("=======================");
        System.out.println(UVCount);
    }

    private static void onlinePV(JavaPairDStream lines) {
        System.out.println("这里执行了吗？");
        // 后面的操作，就像处理 RDD。事实上是 DStream, DStream 是RDD 的父类、模板
        JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String[] logs = v1._2.split("\t");
                String action = logs[5].trim();
                if ("View".equals(action)) {
                    return true;
                } else {
                    return false;
                }
            }
        });


        JavaPairDStream<Long, Long> pairs = logsDStream.mapToPair(new PairFunction<Tuple2<String, String>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, String> t) throws Exception {
                String[] logs = t._2.split("\t");
                Long pageId = Long.valueOf(logs[3]);
                return new Tuple2<Long, Long>(pageId, 1L);
            }
        });

        JavaPairDStream<Long, Long> PVCount = pairs.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        // 这里的 print 不会发出 Job
        // JOB 是基于 Durations 时间间格来触发
        // 必须要有动作，否则代码不执行
        // print, saveAsTextFile, saveAsHadoopFiles,foreachRDD
        // 最重要的是 foreachRDD,因为结果，一般会存储在 Redis, DB,DashBoard
        PVCount.print();
        System.out.println("=======================");
        System.out.println(PVCount);
    }
}
