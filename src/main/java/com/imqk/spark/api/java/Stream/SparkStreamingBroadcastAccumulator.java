package com.imqk.spark.api.java.Stream;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


/**
 * Created by 428900 on 2017/5/14.
 *
 */
public class SparkStreamingBroadcastAccumulator {

    private static volatile Broadcast<List<String>> broadcastList = null;
    private static volatile Accumulator<Integer> accumulator = null;

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingBroadcastAccumulator");
        conf.setMaster("local[2]");

        // 启用多个 StreamingContext 时，需要将前面的关闭，即同一时刻只有一个 StreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 广播 LIST
        broadcastList = ssc.sparkContext().broadcast(Arrays.asList("Hadoop", "Mahout", "Hive"));

        // 全局计数器
        accumulator = ssc.sparkContext().accumulator(0, "OnLineBlackListCounter");

        JavaReceiverInputDStream lines = ssc.socketTextStream("master", 9999);






        JavaPairDStream pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
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


        // 这一段总是报错。
        // 过滤黑名单
        /*wordsCount.foreachRDD(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> rdd, Time time) throws Exception {
                rdd.filter(new Function<Tuple2<String, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Integer> wordPair) throws Exception {
                        if (broadcastList.value().contains(wordPair._1())) {
                            accumulator.add(wordPair._2());
                            return false;
                        } else {
                            return null;
                        }
                    }
                }).collect();

                System.out.println(broadcastList.value().toString() + " : " + accumulator.value());
                return "YES";
            }
        });*/


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
