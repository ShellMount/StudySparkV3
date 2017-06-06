package com.imqk.spark.api.java.Sql;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * 论坛数据自动生成代码
 * 也可以是其它数据
 * 格式：
 * date: yyyy-MM-dd
 * timestamp: 18:00:09
 * userid: shellmount
 * pageID: page.html
 * chanelID: 88
 * action: 点击和注册
 *
 * 这是一个时间，字符串，函数，文件操作 等方法使用的一个好例子
 *
 * Created by 428900 on 2017/5/12.
 */
public class SparkSQKDataManually {
    static String[] channelNames = new String[]{
            "Spark", "Scala", "Hbase", "Hive", "Impala",
            "ML", "Kafaka", "Flink", "com/imqk/spark/api/java/Stream"
    };

    static String[] actionNames = new String[]{
            "View", "Register"
    };

    // 昨天的时间
    static String yesterdayFormated = yesterday();

    // 结果存放内存
    static StringBuffer userLogBuffer = new StringBuffer();

    static String path = "F:\\STUDY\\OutPutApp\\manuallog.txt";

    public static void main(String[] args) {

        long numberItems = 2000000; // 无法生成1亿条数据，内存不够，增加运行配置 -verbose:gc -Xms10G -Xmx10G -Xss128k -XX:+PrintGCDetails

        if (args.length > 0) {
            numberItems = Integer.valueOf(args[0]);
        }

        //System.out.println("生成数据条数：" + numberItems);

        userLogs(numberItems, path);

    }

    private static void userLogs(long numberItems, String path) {
        Random random = new Random();
        for (int i = 0; i < numberItems; i++){
            long timestamp = new Date().getTime();
            long userID = 0L;
            long pageID = 0L;

            userID = random.nextInt((int) numberItems);
            pageID = random.nextInt((int) numberItems);
            String channel = channelNames[random.nextInt(9)];
            String action = actionNames[random.nextInt(2)];

            userLogBuffer.append(yesterdayFormated)
                    .append("\t")
                    .append(timestamp)
                    .append("\t")
                    .append(userID)
                    .append("\t")
                    .append(pageID)
                    .append("\t")
                    .append(channel)
                    .append("\t")
                    .append(action)
                    .append("\n");
        }

        System.out.println(userLogBuffer.toString());

        // 数据写入文件
        PrintWriter printWriter = null;
        try {
            printWriter = new PrintWriter(new OutputStreamWriter(
                    new FileOutputStream(path)
            ));
            printWriter.write(userLogBuffer.toString()); // 这是覆盖写入，如何追加写入？
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            printWriter.close();
        }
    }

    private static String yesterday() {
        /**
         * 计算昨天
         */
        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -1);

        Date yesterday = cal.getTime();
        return date.format(yesterday);
    }
}
