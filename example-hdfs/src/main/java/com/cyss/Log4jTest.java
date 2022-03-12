package com.cyss;

import org.apache.log4j.Logger;

/**
 * @ProjectName: HadoopLearn
 * @PackageName: com.cyss
 * @Author: cyss
 * @CreatTime: 2022-03-03 17:08
 * @Description:
 */
public class Log4jTest {
    public static void main(String[] args) {
        Logger logger = Logger.getLogger(Log4jTest.class);
        logger.debug("这是DEBUG日志");
        logger.info("这是INFO日志");
        logger.warn("这是WARN日志");
        logger.error("这是ERROR日志");
    }
}
