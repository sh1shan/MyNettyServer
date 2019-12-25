package com.dragon.netty.helper.watchers;

import com.dragon.netty.helper.interfaces.ICloseable;
import com.dragon.netty.helper.interfaces.IInitable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

import static com.dragon.netty.utils.ThreadsUtils.createSyncPool;
import static com.dragon.netty.utils.ThreadsUtils.syncClosePool;

/**
 * 公共监听线程池
 */
public class WatchThreadPool implements IInitable, ICloseable {
    private final static Logger logger = LoggerFactory.getLogger(WatchThreadPool.class);

    //线程池对象
    private ThreadPoolExecutor pool = null;

    //线程池配置
    private final int CORE_POOL_SIZE = 5;
    private final int MAX_POOL_SIZE = 10;
    private final int KEEPALIVE_TIME = 1;

    /**
     * 线程池的关闭
     */
    @Override
    public void close() {
        syncClosePool(pool);
        logger.info("[{}]关闭", this.getClass().getSimpleName());
    }

    /**
     * 线程池的初始化
     */
    @Override
    public void init() {
        this.pool = createSyncPool(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEPALIVE_TIME);
        logger.info("监听线程池启动，最大容量[{}]", MAX_POOL_SIZE);
    }

    /**
     * 添加任务
     * 线程池满了不能添加
     *
     * @param runner 需要添加的线程
     */
    public synchronized void addRunner(Runnable runner) {
        if (pool != null) {
            //检查任务是否可行
            String runName = runner.getClass().getSimpleName();
            if (pool.getActiveCount() < MAX_POOL_SIZE) {
                pool.execute(runner);
                logger.info("监听线程池添加任务[{}]成功", runName);

            } else {
                logger.warn("监听线程池添加任务[{}]失败：任务数超过最大线程数[{}]", runName, MAX_POOL_SIZE);
            }
        } else {
            logger.error("监听线程池添加任务失败：pool为null");
        }
    }

}
