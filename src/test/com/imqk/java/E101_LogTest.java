package com.imqk.java;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

// 日志测试
// 需要使用到 F:\STUDY\mylib\log4j-1.2-api-2.8.2.jar
// 需要使用到 log4j.properties

// 1, 将log4j.properties文件放在工程src/main/resources目录下
// 2, 在JAR文件启动参数中添加log4j.properties文件路径
//      -Dlog4j.configuration=<FILE_PATH>
// 3, main 的第一行： PropertyConfigurator.configure("/log4j.properties");



public class E101_LogTest {
    private static Logger logger = Logger.getLogger(E101_LogTest.class);
    
    public static void main (String[] args) {
        //PropertyConfigurator.configure("F:\\STUDY\\APPCODE\\JAVA\\Examples\\log4j.properties");

        // 记录debug级别的信息
        logger.debug("This is debug message.");
        // 记录info级别的信息
        logger.info("This is info message.");
        // 记录error级别的信息
        logger.error("This is error message.");
    }
}