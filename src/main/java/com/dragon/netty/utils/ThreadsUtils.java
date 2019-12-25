package com.dragon.netty.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadsUtils {
    private final static Logger logger = LoggerFactory.getLogger(ThreadsUtils.class);

    /**
     * 线程睡眠
     *
     * @param mills 睡眠时间
     */
    public static void threadSleep(long mills) {
        try {
            if (mills > 0) {
                Thread.sleep(mills);
            }
        } catch (InterruptedException e) {
            logger.warn("线程[{}]休眠异常", Thread.currentThread().getName(), e);
        }
    }

    /**
     * 异步的方式创建线程数池
     *
     * @param corePoolSize  池的数
     * @param maxPoolSize   池的最多容纳数
     * @param keepAliveTime 存活的时间
     * @return
     */
    public static ThreadPoolExecutor createSyncPool(int corePoolSize, int maxPoolSize, int keepAliveTime) {
        ThreadPoolExecutor.CallerRunsPolicy policy = new ThreadPoolExecutor.CallerRunsPolicy();
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, new SynchronousQueue<>(), policy);
    }

    public static ThreadPoolExecutor createPool(int corePoolSize, int maxPoolSize, int keepAliveTime, int queueCapacity) {
        LinkedBlockingDeque queue = new LinkedBlockingDeque(queueCapacity);
        ThreadPoolExecutor.CallerRunsPolicy policy = new ThreadPoolExecutor.CallerRunsPolicy();
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, queue, policy);
    }

    /**
     * 关闭线程池
     *
     * @param executor 执行的线程
     */
    public static void syncClosePool(ThreadPoolExecutor executor) {
        if (executor != null) {
            //不再添加新任务到线程池
            executor.shutdown();
            //延迟关闭超时则强制关闭
            if (!executor.isTerminated()) {
                try {
                    executor.shutdown();
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("线程池强制关闭异常，异常信息: " + e.getMessage(), e);
                    }
                }
            } else {
                logger.warn("线程池为null,不执行关闭!");
            }
        }
    }
}
