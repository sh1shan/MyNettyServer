package com.dragon.netty.helper;

import com.dragon.netty.helper.interfaces.ICloseable;
import com.dragon.netty.helper.interfaces.IInitable;
import com.dragon.netty.helper.watchers.WatchThreadPool;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dragon.netty.constant.CommonConstant.REDIS_KEY_TABLE_MAPPING_NAME;
import static com.dragon.netty.constant.NettyConstant.TOPO_REGISTER_ZK_PATH;
import static com.dragon.netty.utils.CommonUtils.closees;

/**
 * 通用服务
 * 必须跟helper放在一个包
 * helper类服务方法为default
 * 统一由该类提供
 */
public class CommonService implements IInitable, ICloseable {
    private static final Logger logger = LoggerFactory.getLogger(CommonService.class);
    //监听线程池
    private WatchThreadPool watchPool;
    //zk地址服务
    private ZkAddressHelper zkAddressHelper;
    //redis缓存服务
    private RedisCacheHelper redisCacheHelper;
    //httpClient服务
    private HttpClientHelper httpClientHelper;
    //hbase连接服务
    private HbaseConnHelper hbaseConnHelper;

    public static String getTopoUrl() {
        return null;
    }

    @Override
    public void init() {

    }

    /**
     * 初始化所有监听器组件
     */
    private void initHelpers() {
        this.watchPool = new WatchThreadPool();
        watchPool.init();
        this.zkAddressHelper = new ZkAddressHelper(watchPool, TOPO_REGISTER_ZK_PATH);
        zkAddressHelper.init();
        this.redisCacheHelper = new RedisCacheHelper(watchPool, REDIS_KEY_TABLE_MAPPING_NAME);
        redisCacheHelper.init();
        this.httpClientHelper = new HttpClientHelper();
        httpClientHelper.init();
    }

    /**
     * 获取缓存拓扑地址
     *
     * @return
     */
    public String getTopUrl() {
        return zkAddressHelper.getRandomUrl();
    }

    /**
     * 获取httpClient请求超时时间
     */
    public int getHttpReadTimeOut() {
        return httpClientHelper.getReadTimeout();
    }

    /**
     * 响应httpPost请求
     */
    public String getPostResponse(String url, String data) {
        return httpClientHelper.getReponse(url, data);
    }

    /**
     * 获取redis缓存表名
     */
    public String getRedisTableName(String name) {
        return redisCacheHelper.getMappingValue(name);
    }

    /**
     * 获取hbase
     */
    public Connection getHbaseConnection() {
        return hbaseConnHelper.getHbaseConn();
    }

    @Override
    public void close() {
        //关闭组件
        closees(zkAddressHelper, redisCacheHelper, hbaseConnHelper, httpClientHelper);
        //最后关闭监听线程
        closeWachPool();
        logger.info("[{}]关闭", this.getClass().getSimpleName());
    }

    private void closeWachPool() {
        if (null != watchPool) {
            watchPool.close();
        } else {
            logger.warn("watchPool关闭异常：watchPool为null!");
        }
    }

}
