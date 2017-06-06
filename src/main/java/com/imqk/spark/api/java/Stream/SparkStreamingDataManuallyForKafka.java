package com.imqk.spark.api.java.Stream;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import kafka.producer.ProducerConfig;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * 向KAFKA发送消息
 * 论坛数据自动生成代码: 动态不停地生成
 * 也可以是其它数据
 * 格式：
 * date: yyyy-MM-dd
 * timestamp: 18:00:09
 * userid: shellmount
 * pageID: page.html
 * chanelID: 88
 * action: 点击和注册
 * <p>
 * 这是一个时间，字符串，函数，文件操作 等方法使用的一个好例子
 * <p>
 * Created by 428900 on 2017/5/12.
 */
public class SparkStreamingDataManuallyForKafka extends Thread {
    static String[] channelNames = new String[]{
            "Spark", "Scala", "Hbase", "Hive", "Impala",
            "ML", "Kafaka", "Flink", "com/imqk/spark/api/java/Stream"
    };

    static String[] actionNames = new String[]{
            "View", "Register"
    };

    private String topic;   // 发送给KAFKA的数据类别
    private Producer<Integer, String> producerForKafka;
    private static String dateToday;
    private static Random random;

    public SparkStreamingDataManuallyForKafka(String topic) {

        dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        this.topic = topic;
        random = new Random();
        Properties conf = new Properties();
        conf.put("metadata.broker.list", "master:9092,worker-1:9092,worker-2:9092");
        conf.put("serializer.class", "kafka.serializer.StringEncoder");

        producerForKafka = new Producer<Integer, String>(new ProducerConfig(conf));
    }

    @Override
    public void run() {
        int counter = 0;
        while (true) {
            counter++;
            String userLog = userLogs();
            System.out.println("product: " + userLog);

            producerForKafka.send(new KeyedMessage<Integer, String>(topic, userLog));

            try {
                if (counter % 500 == 0) {
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {


        new SparkStreamingDataManuallyForKafka("testMyFirstKafkaMessage").start();
        // 应该是下面这行，但为了方便（懒得启动KAFKA服务），使用上面这行
        //new SparkStreamingDataManuallyForKafka("UserLogs").start();

    }

    private static String userLogs() {
        StringBuffer userLogBuffer = new StringBuffer("");
        int[] unregisteredUsers = new int[]{1, 2, 3, 4, 5, 6, 7, 8};
        long timestamp = new Date().getTime();
        long userID = 0L;
        long pageID = 0L;

        // 随机生成用户ID
        if (unregisteredUsers[random.nextInt(8)] == 1) {
            userID = -1;
        } else {
            userID = (long) random.nextInt(2000);
        }
        pageID = random.nextInt(2000);
        String channel = channelNames[random.nextInt(9)];
        String action = actionNames[random.nextInt(2)];

        userLogBuffer.append(dateToday)
                .append("\t")
                .append(timestamp)
                .append("\t")
                .append(userID)
                .append("\t")
                .append(pageID)
                .append("\t")
                .append(channel)
                .append("\t")
                .append(action);
                //.append("\n");

        // 返回结果
        return userLogBuffer.toString();
    }


}
