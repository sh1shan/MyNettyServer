package com.dragon.netty.helper.watchers;

import com.dragon.netty.helper.interfaces.IAnswerService;
import com.dragon.netty.helper.interfaces.ICloseable;
import com.dragon.netty.helper.interfaces.IInitable;
import com.dragon.netty.helper.interfaces.IWatchable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dragon.netty.utils.ThreadsUtils.threadSleep;

public class ZkEmptyWatcher implements Runnable, IWatchable, ICloseable, IInitable {
    private static final Logger logger = LoggerFactory.getLogger(ZkEmptyWatcher.class);

    //监听周期间隔
    private static final long SLEEP_MILLS = 3 * 60 * 1000;

    //启动标识
    private volatile boolean open = false;

    // 通知对象
    private IAnswerService helper;

    public ZkEmptyWatcher(IAnswerService _helper) {
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
        logger.info("通知zk服务定时检测拓扑地址列表是否为空！");
        this.helper.answerAndOperate(this);
    }

    @Override
    public void run() {
        while (open) {
            threadSleep(SLEEP_MILLS);
            watchAndNotify();
        }
    }
}
