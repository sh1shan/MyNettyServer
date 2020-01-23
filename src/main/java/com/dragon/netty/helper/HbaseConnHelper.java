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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class HbaseConnHelper implements IInitable, Iloadable, ICloseable, IAnswerService {
    private static final Logger logger= LoggerFactory.getLogger(HbaseConnHelper.class);

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

    public HbaseConnHelper(WatchThreadPool _watchPool){
        this.watchPool=_watchPool;
    }

    @Override
    public void initWatchers() {

    }

    @Override
    public void answerAndOperate(IWatchable watchable) {

    }

    @Override
    public void close() {

    }

    @Override
    public void init() {
        loadConfig();
        initConn();
        initWatchers();

    }

    private void initConn() {
        if ()
    }

    @Override
    public void loadConfig() {
        this.useKerberos= Objects.equals(PropertyUtils.getProp("kerberos.open"),"true");
        this.reloginInited=Objects.equals(PropertyUtils.getProp("hbase.relogin.watch.init"),"true");
        this.reloginHours = Integer.parseInt(PropertyUtils.getProp("hbase.relogin.interval.hour"));
        this.conf=(useKerberos? HBaseUtils.initHbaseConfKerberos():HBaseUtils.initHbaseConf());
    }
}
