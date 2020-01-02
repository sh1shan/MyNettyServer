package com.dragon.netty.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.dragon.netty.utils.CommonUtils.awayCurrentMills;
import static java.lang.System.currentTimeMillis;

public class ThreadsUtils {
    private final static Logger logger = LoggerFactory.getLogger(ThreadsUtils.class);

    //平均等待毫秒
    private final static int PER_WAIT_MILLS = 5;
    //最大等待毫秒
    private final static int MAX_CLOSE_WAIT_MILLS = 5 * 1000;


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

    /**
     * 创建线程池
     *
     * @param corePoolSize
     * @param maxPoolSize
     * @param keepAliveTime
     * @param queueCapacity
     * @return
     */
    public static ThreadPoolExecutor createPool(int corePoolSize, int maxPoolSize, int keepAliveTime, int queueCapacity) {
        LinkedBlockingDeque queue = new LinkedBlockingDeque(queueCapacity);
        ThreadPoolExecutor.CallerRunsPolicy policy = new ThreadPoolExecutor.CallerRunsPolicy();
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, queue, policy);
    }

    public static void awaitClosePool(ThreadPoolExecutor executor) {
        if (executor != null) {
            //不再添加新任务到线程池
            executor.shutdown();
            //关闭线程池：当消息处理线程池还有任务的时候延迟关闭直到任务结束或达到超时时长
            long start = currentTimeMillis();
            while (!executor.isTerminated() && awayCurrentMills(start) < MAX_CLOSE_WAIT_MILLS) {
                try {
                    executor.awaitTermination(PER_WAIT_MILLS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.warn("线程池await异常", e);
                }
            }
        }
        //延迟关闭超时则强制关闭
        if (!executor.isTerminated()) {
            logger.warn("线程延迟关闭超时，超时时长:[{}]毫秒，强制关闭", MAX_CLOSE_WAIT_MILLS);
            try {
                executor.shutdown();
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("线程池强制关闭,异常信息:" + e.getMessage(), e);
                }
            }
        } else {
            logger.warn("线程为null,不执行关闭");
        }
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
