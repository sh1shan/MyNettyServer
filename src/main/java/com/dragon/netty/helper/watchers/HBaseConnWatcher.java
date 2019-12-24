package com.dragon.netty.helper.watchers;


import com.dragon.netty.helper.interfaces.IAnswerService;
import com.dragon.netty.helper.interfaces.ICloseable;
import com.dragon.netty.helper.interfaces.IInitable;
import com.dragon.netty.helper.interfaces.IWatchable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 定时认证HBase连接
 */
public class HBaseConnWatcher implements Runnable, ICloseable, IWatchable, IInitable {
    private final static Logger logger = LoggerFactory.getLogger(HBaseConnWatcher.class);

    //认证间隔
    private final int sleepHours;

    //启动标识
    private volatile boolean open = false;

    //通知对象
    private IAnswerService helper;

    public HBaseConnWatcher(IAnswerService _helper, int _sleepHours) {
        this.helper = _helper;
        this.sleepHours = _sleepHours;
    }

    @Override
    public void close() {
        this.open = false;
        logger.info("[{}]关闭", this.getClass().getSimpleName());
    }

    @Override
    public void init() {
        this.open = true;
    }

    @Override
    public void watchAndNotify() {
        logger.info("通知HBase服务定时认证");
        this.helper.answerAndOperate(this);
    }

    @Override
    public void run() {
        while (open) {
            try {
                TimeUnit.HOURS.sleep(sleepHours);
            } catch (InterruptedException e) {
                logger.error("线程休眠异常,异常信息： " + e.getMessage(), e);
            }
            watchAndNotify();
        }
    }
}
