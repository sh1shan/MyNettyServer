package com.dragon.netty.helper.watchers;

import com.dragon.netty.helper.interfaces.IAnswerService;
import com.dragon.netty.helper.interfaces.ICloseable;
import com.dragon.netty.helper.interfaces.IInitable;
import com.dragon.netty.helper.interfaces.IWatchable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dragon.netty.utils.ThreadsUtils.threadSleep;
import static com.dragon.netty.constant.CommonConstant.COMM_SLEEP_MILLS;

/**
 * 定时缓存redis数据
 * 加载前先检查连接是否可用
 */
public class RedisCacheWacher implements Runnable, ICloseable, IWatchable, IInitable {

    private final static Logger logger = LoggerFactory.getLogger(RedisCacheWacher.class);

    //启动标识
    private volatile boolean open = false;

    //通知对象
    private IAnswerService helper;

    public RedisCacheWacher(IAnswerService _helper) {
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
        logger.info("通知redis服务定时加载映射关系");
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
