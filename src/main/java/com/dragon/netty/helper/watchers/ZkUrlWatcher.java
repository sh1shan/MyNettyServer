package com.dragon.netty.helper.watchers;

import com.dragon.netty.helper.ZkAddressHelper;
import com.dragon.netty.helper.interfaces.ICloseable;
import com.dragon.netty.helper.interfaces.IInitable;
import com.dragon.netty.helper.interfaces.IWatchable;
import com.dragon.netty.utils.URLCheckUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.LinkedBlockingDeque;

import static com.dragon.netty.constant.CommonConstant.RETRY_SLEEP_MILLS;
import static com.dragon.netty.utils.CommonUtils.awayCurrentMills;
import static com.dragon.netty.utils.ThreadsUtils.threadSleep;


/**
 * 检查缓存的zk上的拓扑地址是否相同
 * 没周期监听一定时间，间歇性监控
 * 周期内有不可用url强制启动一次重载逻辑
 */
public class ZkUrlWatcher implements Runnable, ICloseable, IWatchable, IInitable {
    private static final Logger logger = LoggerFactory.getLogger(ZkPathWatcher.class);

    //错误码
    private final String ERROR_STR = UUID.randomUUID().toString();

    //启动标识
    private volatile boolean open = false;

    //监听周期间隔
    private static final long PER_RANGE_MILLS = 60 * 1000;

    //监听持续时间：最少100分之一 最大三分之一周期
    private static final long CHECK_RANGE_MILLS = checkRangeMills(6 * 1000);

    //监听器初始化时间，延后一个周期
    private final long START_TS = System.currentTimeMillis() + PER_RANGE_MILLS;

    //监听器上次通知时间：每notify一次更新一次
    private volatile long lastNotifyTs = System.currentTimeMillis() + PER_RANGE_MILLS;

    //队列最大长度
    public static final int MAX_QUE_SIZE = 10000;
    private final ZkAddressHelper helper;
    private final LinkedBlockingDeque<String> urlQue;

    public ZkUrlWatcher(ZkAddressHelper _helper, LinkedBlockingDeque<String> _urlQue) {
        this.helper = _helper;
        this.urlQue = _urlQue;
    }

    private static long checkRangeMills(long inmills) {
        return Math.min((int) (PER_RANGE_MILLS / 3), Math.max((int) (PER_RANGE_MILLS / 100), inmills));
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
        String url = takeUrlFromUeq();
        //如果再检查周期切正常拉取则检查是否可用，如果不可用则通知
        if (isCheckAble() && !ERROR_STR.equals(url) && URLCheckUtils.isUrlAvailable(url)) {
            logger.warn("url[{}] isn't available", url);
            //清掉队列数据，获取最新的数据再检查一次
            threadSleep(RETRY_SLEEP_MILLS);
            clearUrlFromQue();
            String secondUrl = takeUrlFromUeq();
            if (!ERROR_STR.equals(url) && !URLCheckUtils.isUrlAvailable(url)) {
                logger.error("second url [{}] isn't available");
                if (awayCurrentMills(lastNotifyTs) > PER_RANGE_MILLS) {
                    logger.error("检查最新的url[{}] 无效，通知服务重新加载", secondUrl);
                    this.helper.answerAndOperate(this);

                    threadSleep(RETRY_SLEEP_MILLS);
                    updateNotify();
                    clearUrlFromQue();

                }
            }
        }
    }

    /**
     * 更新ts
     */
    private void updateNotify() {
        this.lastNotifyTs = System.currentTimeMillis();
    }

    private void clearUrlFromQue() {
        try {
            this.urlQue.clear();
        } catch (Exception e) {
            logger.warn("clear url queue fail ,msg " + e.getMessage(), e);
        }
    }

    /**
     * 判断当前时间是否为检查时间
     * 计算方式如下：
     * 当前时间减去启动时间的结果大于0
     * 且结果对周期取模余数小于CHECK_RANGE_MILLS
     * 则证明要进行检查
     *
     * @return
     */
    private boolean isCheckAble() {
        long awayMills = awayCurrentMills(START_TS);
        return awayMills > 0 && ((awayMills % PER_RANGE_MILLS) < CHECK_RANGE_MILLS);
    }

    private String takeUrlFromUeq() {
        String url;
        try {
            url = this.urlQue.take();
        } catch (InterruptedException e) {
            //如果发生异常返回错误码
            url = ERROR_STR;
            logger.warn("监听器队列拉取发生异常,异常信息" + e.getMessage(), e);
        }
        return url;
    }

    @Override
    public void run() {
        while (open) {
            watchAndNotify();
        }
    }
}
