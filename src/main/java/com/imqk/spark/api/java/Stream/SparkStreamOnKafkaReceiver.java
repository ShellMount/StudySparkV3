package com.imqk.spark.api.java.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Created by 428900 on 2017/5/14.
 *
 */
public class SparkStreamOnKafkaReceiver {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamOnKafkaReceiver");
        // Executor Core 最佳分配为奇数个，3，5，7
        conf.setMaster("local[4]");
        //conf.setMaster("spark://sparkmaster:7077");

        // 启用多个 StreamingContext 时，需要将前面的关闭，即同一时刻只有一个 StreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(15));

        /*
            数据来源，可以是 file, 网络，HDFS，FLUME，KAFKA，SOCKET
            监听时间段没有数据，也会跑一个JOB，造成资源浪费
         */
        //JavaReceiverInputDStream lines = ssc.socketTextStream("master", 9999);
        // 参数：ssc, zookeerper集群信息, consumer Group, 消费TOPIC及并发读取的并发线程数
        Map<String, Integer> topicConsumerConcurrency = new HashMap<String, Integer>();
        topicConsumerConcurrency.put("testMyFirstKafkaMessage", 2);

        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(ssc,
                "master:2181,worker-1:2181,worker-2:2181",
                "TEST",
                topicConsumerConcurrency);

        // 后面的操作，就像处理 RDD。事实上是 DStream, DStream 是RDD 的父类、模板
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

            //@Override
            public Iterator<String> call(Tuple2<String, String> tuple) throws Exception {
                return Arrays.asList(tuple._2.split(" ")).iterator();
            }
        });

        JavaPairDStream pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            //@Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //@Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 这里的 print 不会发出 Job
        // JOB 是基于 Durations 时间间格来触发
        // 必须要有动作，否则代码不执行
        // print, saveAsTextFile, saveAsHadoopFiles,foreachRDD
        // 最重要的是 foreachRDD,因为结果，一般会存储在 Redis, DB,DashBoard
        wordsCount.print();
        System.out.println("=======================");
        System.out.println(wordsCount);

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
