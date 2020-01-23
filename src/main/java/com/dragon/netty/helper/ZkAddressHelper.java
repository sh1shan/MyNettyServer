package com.dragon.netty.helper;

import com.dragon.netty.helper.interfaces.*;
import com.dragon.netty.helper.watchers.*;
import com.dragon.netty.utils.PropertyUtils;
import com.dragon.netty.utils.ZKClient;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.security.SecureRandom;

import static com.dragon.netty.constant.CommonConstant.CONF_ZOOKEEPER_ADDRESS;
import static com.dragon.netty.constant.CommonConstant.RETRY_MAX_TIME;
import static com.dragon.netty.constant.CommonConstant.RETRY_SLEEP_MILLS;
import static com.dragon.netty.utils.CommonUtils.closees;
import static com.dragon.netty.utils.ThreadsUtils.threadSleep;


public class ZkAddressHelper implements IInitable, ICloseable, IAnswerService, Iloadable {
    private final static Logger logger = LoggerFactory.getLogger(ZkAddressHelper.class);

    //zk连接地址
    private String zkConnAddress;

    //监听线路径
    private final String watchPath;

    //监听线程池
    private final WatchThreadPool watchPool;

    //zkClient
    private volatile ZKClient zkClient;

    //路径监听器
    private volatile ZkPathWatcher pathWatcher;

    //客户端监听器
    private ZkClientWatcher clientWatcher;

    //url监听器
    private ZkUrlWatcher urlWatcher;

    //列表空监听器
    private ZkEmptyWatcher emptyWatcher;

    //对象公用同一把锁
    private final Lock thisLock = new ReentrantLock();

    //初始化容量
    private final int initCapacity = 50;

    //缓存所有拓扑注册地址
    private final List<String> topoUrlList = new ArrayList<>(initCapacity);

    //监听器传递url的队列：有界，阻塞，线程安全
    private final LinkedBlockingDeque<String> urlQue = new LinkedBlockingDeque<>(ZkUrlWatcher.MAX_QUE_SIZE);

    public ZkAddressHelper(WatchThreadPool _watchPool, String _watchPath) {
        this.watchPool = _watchPool;
        this.watchPath = _watchPath;
    }


    @Override
    public void initWatchers() {
        this.clientWatcher = new ZkClientWatcher(this);
        this.urlWatcher = new ZkUrlWatcher(this, urlQue);
        this.emptyWatcher = new ZkEmptyWatcher(this);
        //打开监听
        this.clientWatcher.init();
        this.urlWatcher.init();
        this.emptyWatcher.init();
        //异步启动
        watchPool.addRunner(clientWatcher);
        watchPool.addRunner(urlWatcher);
        watchPool.addRunner(emptyWatcher);
    }

    /**
     * 监听方法
     * 根据请求不同执行不同逻辑
     */
    @Override
    public void answerAndOperate(IWatchable watcher) {
        if (watcher != null) {
            String watcherName = watcher.getClass().getSimpleName();
            logger.info("接收到的监听器[{}]通知", watcherName);

            if (watcher.equals(clientWatcher)) {
                if (!isZkClientAvailable()) {
                    logger.warn("监听到zk客户端不可用，重连zkClient");
                    reConnectZkClient();
                }
            } else if (watcher.equals(pathWatcher)) {
                logger.warn("监听到拓扑地址变化");
                loadUrlList();
            } else if (watcher.equals(urlWatcher)) {
                logger.warn("监听地址不可用,重连zkClient、重新加载地址");
                reConnectZkClient();
                loadUrlList();
            } else if (watcher.equals(emptyWatcher)) {
                if (isTopoUrlListEmpty()) {
                    logger.warn("监听拓扑地址为空，重启zkClient、重新加载地址");
                    closeZkClient();
                    initZkClient();
                }
            } else {
                logger.warn("未匹配传入监听器[{}]", watcher.getClass().getSimpleName());
            }
            logger.info("响应监听器[{}]通知完毕", watcherName);
        } else {
            logger.warn("传入监听器为null");
        }
    }

    /**
     * 监听topo列表是否为空
     * 非空检查1次
     * 空检查3次
     *
     * @return
     */
    private boolean isTopoUrlListEmpty() {
        boolean isEmpty = true;
        int retryTimes = 0;
        while (isEmpty && retryTimes < RETRY_MAX_TIME) {
            retryTimes++;
            try {
                isEmpty = topoUrlList.isEmpty();
            } catch (Exception e) {
                isEmpty = false;
                threadSleep(RETRY_SLEEP_MILLS);
            }
        }
        logger.info("topoUrlList:{}", topoUrlList.toString());
        return isEmpty;
    }

    /**
     * 关闭zkClient与pathwatcher
     * 重试3次
     */
    private void closeZkClient() {
        boolean flg = false;
        int retryTime = 0;
        //重试 每次重试后休眠
        while (!flg && retryTime < RETRY_MAX_TIME) {
            retryTime++;
            try {
                zkClient.close();
                flg = true;
                logger.info("zkClient 关闭成功");
            } catch (InterruptedException e) {
                logger.warn("zkClient关闭发生异常，异常信息:" + e.getMessage(), e);
            }
        }
    }

    /**
     * 重新连接 zkClient
     * 不检查状态
     */
    private void reConnectZkClient() {
        //尝试加锁
        if (thisLock.tryLock()) {
            try {
                if (!isClientNull()) {
                    boolean isConn = zkClient.reconnect(this.pathWatcher);
                    logger.info("zkClient重连状态[{}]", isConn);
                } else {
                    logger.info("zkclient重连失败:zkClient为null");
                }

            } catch (Exception e) {
                logger.error("zkClient 重连发生异常，异常信息：" + e.getMessage(), e);
            } finally {
                thisLock.unlock();
            }
        } else {
            logger.warn("执行zkClient重连获取锁失败：其他线程已持有锁！");
        }
    }

    /**
     * 检查zk连接是否可用
     * 重试3次
     *
     * @return
     */
    private boolean isZkClientAvailable() {
        boolean rtn = false;
        int retryTime = 0;
        while (!rtn && retryTime < RETRY_MAX_TIME) {
            retryTime++;
            try {
                if (!isClientNull()) {
                    rtn = zkClient.isAvailable();
                } else {
                    logger.error("检查zkClient是否可用失败:zkClient为null");
                }
            } catch (Exception e) {
                logger.warn("检查zkClient是否可用发生异常,异常信息:" + e.getMessage(), e);
                threadSleep(RETRY_SLEEP_MILLS);
            }
        }
        return rtn;
    }

    /**
     * 加载拓扑地址列表
     */
    public void loadUrlList() {
        //尝试加锁
        if (thisLock.tryLock()) {
            logger.info("开始加载拓扑");
            try {
                if (!isClientNull()) {
                    if (zkClient.exist(this.watchPath)) {
                        List<String> ipPorts = zkClient.listService(this.watchPath, true);
                        updateUrllist(ipPorts);
                    } else {
                        logger.warn("加载拓扑地址失败：拓扑注册地址[{}]不存在，请检查拓扑是否重启", this.watchPath);
                    }
                } else {
                    logger.error("加载拓扑地址失败:zkClient为null");
                }
            } catch (Exception e) {
                logger.error("加载地址发生异常，错误信息：" + topoUrlList.toString());
            } finally {
                //释放锁
                thisLock.unlock();
                logger.info("拓扑地址列表:[{}]", topoUrlList.toString());
            }
        } else {
            logger.warn("执行加载拓扑地址获取锁失败：其他线程已经持有锁！");
        }
    }

    /**
     * 替换地址列表
     * 重试多次
     *
     * @param ipPorts
     */
    private void updateUrllist(List<String> ipPorts) {
        boolean flg = false;
        int retryTimes = 0;
        //重试 每次重试后休眠
        while (!flg && retryTimes < RETRY_MAX_TIME) {
            retryTimes++;
            flg = addInPorts(ipPorts);
            if (flg) {
                threadSleep(RETRY_SLEEP_MILLS);

            }
        }
        logger.info("拓扑地址是否替换成功标识:[{}]", flg);
    }

    /**
     * 缓存新地址
     * 先清除旧地址仔添加
     * 如果新地址空不替换
     *
     * @param ipPorts ip地址
     * @return 成功标识
     */
    private boolean addInPorts(List<String> ipPorts) {
        boolean flg = false;
        try {
            if (ipPorts != null && ipPorts.isEmpty()) {
                //可能有并发问题
                topoUrlList.clear();
                topoUrlList.addAll(ipPorts);
                flg = true;
            } else {
                logger.warn("监听器获取的拓扑地址为空");
            }
        } catch (Exception e) {
            logger.warn("替换拓扑地址列表异常，异常信息:" + e.getMessage(), e);
        }
        return flg;
    }

    /**
     * 检查zkClient是否为空
     *
     * @return
     */
    private boolean isClientNull() {
        return (null == zkClient);
    }

    /**
     * 关闭方法
     */
    @Override
    public void close() {
        closees(urlWatcher, clientWatcher, emptyWatcher);
        closeZkClient();
        logger.info("[{}]关闭！", this.getClass().getSimpleName());

    }

    @Override
    public void init() {
        loadConfig();
        initZkClient();
        initWatchers();
    }

    /**
     * zkClient初始化
     * 会自动加载监听地址内容
     */
    private void initZkClient() {
        try {
            this.pathWatcher = new ZkPathWatcher(this, watchPath);
            this.zkClient = new ZKClient(zkConnAddress, pathWatcher);
            logger.info("zkClient初始化完成");
        } catch (IOException e) {
            logger.error("zkClient 初始化发生异常，异常信息：" + e.getMessage(), e);
        }
    }

    @Override
    public void loadConfig() {
        this.zkConnAddress = PropertyUtils.getProp(CONF_ZOOKEEPER_ADDRESS);
    }

    private void putUrlTopoQue(String url) {
        if (!this.urlQue.offer(url)) {
            logger.debug("放入url到监听队列失败：队列已满！");
        }
    }

    /**
     * 随机获取地址
     * 重试多次
     */
    String getRandomUrl() {
        String url = StringUtils.EMPTY;
        int retryTime = 0;
        while (StringUtils.isBlank(url) && retryTime < RETRY_MAX_TIME) {
            retryTime++;
            url = getUrlFromList();
            if (StringUtils.isBlank(url)) {
                threadSleep(RETRY_SLEEP_MILLS);
            }
        }
        if (StringUtils.isBlank(url)) {
            logger.info("重试[{}]次后获取拓扑url仍为空！", RETRY_MAX_TIME);
        } else {
            //监听url
            putUrlTopoQue(url);
        }
        return url;
    }

    private String getUrlFromList() {
        String url = StringUtils.EMPTY;
        try {
            if (!topoUrlList.isEmpty()) {
                SecureRandom ramdom = new SecureRandom();
                //在重新加载杜族瞬间，可能数组越界！
                url = topoUrlList.get(ramdom.nextInt(topoUrlList.size()));
            } else {
                logger.warn("获取拓扑地址失败：地址列表为空，请检查zkClient是否初始化！");
            }
        } catch (Exception e) {
            logger.warn("获取拓扑地址发生异常，请检查是否正在更新，异常信息:" + e.getMessage(), e);
        }
        return url;
    }

}
