package com.dragon.netty.helper;

import com.dragon.netty.helper.interfaces.IAnswerService;
import com.dragon.netty.helper.interfaces.ICloseable;
import com.dragon.netty.helper.interfaces.IInitable;
import com.dragon.netty.helper.interfaces.IWatchable;
import com.dragon.netty.helper.interfaces.Iloadable;
import com.dragon.netty.helper.watchers.HBaseConnWatcher;
import com.dragon.netty.helper.watchers.WatchThreadPool;
import com.dragon.netty.utils.HBaseUtils;
import com.dragon.netty.utils.PropertyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Connection;

import static com.dragon.netty.utils.CommonUtils.closees;

import java.io.IOException;
import java.util.Objects;

public class HbaseConnHelper implements IInitable, Iloadable, ICloseable, IAnswerService {
    private static final Logger logger = LoggerFactory.getLogger(HbaseConnHelper.class);

    //调度线程池
    private final WatchThreadPool watchPool;

    //是否认证
    private boolean useKerberos;

    //认证启动标识
    private boolean reloginInited;

    //认证间隔时间
    private int reloginHours;

    //hbase连接配置
    private Configuration conf;

    //认证信息
    private UserGroupInformation ugi;

    //连接
    private Connection hbaseConn;

    //hbase认证监听器
    private HBaseConnWatcher connWatcher;

    public HbaseConnHelper(WatchThreadPool _watchPool) {
        this.watchPool = _watchPool;
    }

    @Override
    public void initWatchers() {
        if (useKerberos && reloginInited) {
            this.connWatcher = new HBaseConnWatcher(this, this.reloginHours);
            connWatcher.init();
            watchPool.init();
            watchPool.addRunner(connWatcher);
            logger.info("hbase认证监听器启动成功！");
        } else {
            logger.info("hbase连接未开启认证或者监听器启动标志不为true，不启动监听器！");
        }

    }

    @Override
    public void answerAndOperate(IWatchable watcher) {
        if (null != watcher) {
            String watcherName = watcher.getClass().getSimpleName();
            logger.info("接收到监听器[{}]通知 ", watcherName);
            if (watcherName.equals(connWatcher)) {
                HBaseUtils.reConfirKerberos(ugi);
            } else {
                logger.warn("未匹配监听器：[{}]", watcherName);
            }
            logger.info("响应监听器[{}]通知完毕", watcherName);
        } else {
            logger.warn("传入监听器为null");
        }

    }

    @Override
    public void close() {
        closees(connWatcher);
        closeHbaseConn();
    }

    private void closeHbaseConn() {
        if (null != hbaseConn) {
            try {
                hbaseConn.close();
            } catch (IOException e) {
                logger.warn("关闭hbase连接异常！异常信息：[{}]", e.getMessage(), e);
            }
        } else {
            logger.warn("hbase连接关闭失败：连接为null");
        }
    }

    @Override
    public void init() {
        loadConfig();
        initConn();
        initWatchers();

    }

    private void initConn() {
        if (useKerberos) {
            this.ugi = HBaseUtils.confirkerberos(conf);
        }
//        this.hbaseConn = HBaseUtils.initHbaseConf();
    }

    @Override
    public void loadConfig() {
        //初始化hbase信息
        this.useKerberos = Objects.equals(PropertyUtils.getProp("kerberos.open"), "true");
        this.reloginInited = Objects.equals(PropertyUtils.getProp("hbase.relogin.watch.init"), "true");
        this.reloginHours = Integer.parseInt(PropertyUtils.getProp("hbase.relogin.interval.hour"));
        this.conf = (useKerberos ? HBaseUtils.initHbaseConfKerberos() : HBaseUtils.initHbaseConf());
    }

    /**
     * 获取hbase连接
     *
     * @return
     */
    Connection getHbaseConn() {
        return hbaseConn;
    }
}
