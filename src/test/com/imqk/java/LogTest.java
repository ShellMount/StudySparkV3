package com.imqk.java;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created by 428900 on 2017/5/25.
 */

public class LogTest {
    private static Logger logger = Logger.getLogger(LogTest.class);

    public static void main (String[] args) {
        PropertyConfigurator.configure("F:\\STUDY\\APPCODE\\JAVA\\Examples\\log4j.properties");

        // 记录debug级别的信息
        logger.debug("This is debug message.");
        // 记录info级别的信息
        logger.info("This is info message.");
        // 记录error级别的信息
        logger.error("This is error message.");
    }
}