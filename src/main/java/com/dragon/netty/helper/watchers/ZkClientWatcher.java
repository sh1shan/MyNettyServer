package com.dragon.netty.helper.watchers;


import com.dragon.netty.helper.interfaces.IAnswerService;
import com.dragon.netty.helper.interfaces.ICloseable;
import com.dragon.netty.helper.interfaces.IInitable;
import com.dragon.netty.helper.interfaces.IWatchable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dragon.netty.utils.ThreadsUtils.threadSleep;
import static com.dragon.netty.constant.CommonConstant.COMM_SLEEP_MILLS;

public class ZkClientWatcher implements Runnable, IWatchable, ICloseable, IInitable {

    private final static Logger logger = LoggerFactory.getLogger(ZkClientWatcher.class);

    //启动标识
    private volatile boolean open = false;

    //通知对象
    private IAnswerService helper;

    public ZkClientWatcher(IAnswerService _helper) {
        this.helper = _helper;
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
        logger.info("通知zk服务定时检测zkClient是否有效。");
        this.helper.answerAndOperate(this);
    }

    @Override
    public void run() {
        while (open) {
            threadSleep(COMM_SLEEP_MILLS);
            watchAndNotify();
        }
    }
}
