package com.imqk.spark.api.java.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * Created by 428900 on 2017/5/14.
 *
 * 实现输入数据的累计
 *
 * UpdataStatusByKey 主要是可能随着时间流逝，可以 为SPARK STREAMING中每一份DSTREAM
 * 可维护一份状态，当要更新的时候，可以对已经存在的状态更新
 * 通过更新函数对Status更新时，要是返回为None, 则原来的 key-statue(key) 会被删除掉
 * statues 可以是任意类型的数据结构
 *
 * 不断更新KEY－STATUS时，需要有容错机制，这就需要开启 CheckPoint
 *
 * 通过保存历史数据，就可以对数据进行动态分析了，也就是我们说的热点追踪等功能
 */
public class UpdataStatusByKeyDemo {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("UpdataStatusByKeyDemo");
        // Executor Core 最佳分配为奇数个，3，5，7
        conf.setMaster("local[2]");
        //conf.setMaster("spark://sparkmaster:7077");

        // 启用多个 StreamingContext 时，需要将前面的关闭，即同一时刻只有一个 StreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 后面的 updateStateByKey  必须要求使用 CheckPoint
        ssc.checkpoint("F:\\STUDY\\TempDir\\CheckPoint");

        /*
            数据来源，可以是 file, 网络，HDFS，FLUME，KAFKA，SOCKET
            监听时间段没有数据，也会跑一个JOB，造成资源浪费
         */
        JavaReceiverInputDStream lines = ssc.socketTextStream("master", 9999);

        // 后面的操作，就像处理 RDD。事实上是 DStream, DStream 是RDD 的父类、模板
        //
        JavaDStream words = lines.flatMap(new FlatMapFunction<String, String>() {

            //@Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }

        });

        JavaPairDStream pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            //@Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        /**
         * 要是没有 updateStateByKey 函数，要怎么才能达到同样的效果呢？
         * 如果数据特别多的话，就只能缓存到REDIS或TECHYON内存文件系统中了
         *
         * 也可以联合 mapWithState 使用
         *
         * 下面的功能，会不断累积接收到的数据，并进行累加
         */
        JavaPairDStream<String, Integer> wordsCount = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional call(List<Integer> values, Optional<Integer> state) throws Exception {
                /**
                 * 这里 state ---> value 更像是 old -> new
                 * Optional[1]---->[1]  : 该键值对中，旧数据KEY的值为1，新数据又有1
                 * Optional[1]---->[]   : 该键值对中，旧数据KEY的值为1，新数据无
                 * Optional[2]---->[]   : 该键值对中，旧数据KEY的值为2，新数据无
                  */
                System.out.println(state + "---->" + values);


                Integer updateValue = 0;
                if (state.isPresent()){
                    updateValue = state.get();
                }

                for (Integer value: values){
                    updateValue += value;
                }
                return Optional.of(updateValue);
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
