package com.dragon.netty.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadsUtils {
    private final static Logger logger = LoggerFactory.getLogger(ThreadsUtils.class);


    public static void threadSleep(long mills) {
        try {
            if (mills > 0) {
                Thread.sleep(mills);
            }
        } catch (InterruptedException e) {
            logger.warn("线程[{}]休眠异常", Thread.currentThread().getName(), e);
        }
    }
}
