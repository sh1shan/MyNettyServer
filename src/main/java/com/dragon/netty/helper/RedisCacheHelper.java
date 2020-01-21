package com.dragon.netty.helper;

import com.dragon.netty.helper.interfaces.IAnswerService;
import com.dragon.netty.helper.interfaces.ICloseable;
import com.dragon.netty.helper.interfaces.IInitable;
import com.dragon.netty.helper.interfaces.IWatchable;
import com.dragon.netty.helper.interfaces.Iloadable;
import com.dragon.netty.helper.watchers.RedisCacheWacher;
import com.dragon.netty.helper.watchers.WatchThreadPool;
import com.dragon.netty.utils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.dragon.netty.constant.CommonConstant.CONF_REDIS_YX_HOSTNAME;
import static com.dragon.netty.constant.CommonConstant.CONF_REDIS_YX_PORT;
import static com.dragon.netty.utils.CommonUtils.closees;


/**
 * redis缓存key映射关系
 * 只支持hash格式缓存
 */
public class RedisCacheHelper implements IInitable, ICloseable, IAnswerService, Iloadable {

    private static final Logger logger = LoggerFactory.getLogger(RedisCacheHelper.class);

    //redis连接地址
    private String redisIp;

    //redis连接端口
    private int redisPort;

    //缓存key名
    private final String cacheKey;

    //调度线程池
    private final WatchThreadPool watchpool;

    //缓存加载器
    private RedisCacheWacher cacheWatcher;

    //redis超时时间
    private static final int REDIS_TIMEOUT_MILLS = 60 * 1000;

    //初始化容量
    private final int initCapacity = 20;

    //redis表映射
    private volatile Map<String, String> cacheMap = new ConcurrentHashMap<>(initCapacity);

    public RedisCacheHelper(WatchThreadPool _watchPool, String _cacheKey) {
        this.watchpool = _watchPool;
        this.cacheKey = _cacheKey;
    }

    @Override
    public void initWatchers() {
        this.cacheWatcher = new RedisCacheWacher(this);
        this.cacheWatcher.init();
        watchpool.addRunner(cacheWatcher);

    }

    @Override
    public void answerAndOperate(IWatchable watch) {
        if (null != watch) {
            String watcherName = watch.getClass().getSimpleName();
            logger.info("接收到监听器[{}]通知", watcherName);
            if (watch.equals(cacheWatcher)) {
                loadMapping();
            } else {
                logger.warn("未匹配监听器[{}]", watcherName);
            }
            logger.info("响应监听器[{}]通知完毕", watcherName);
        } else {
            logger.warn("传入监听器为null");
        }
    }

    @Override
    public void close() {
        closees(cacheWatcher);

    }

    @Override
    public void init() {
        loadConfig();
        loadMapping();
        initWatchers();

    }

    /**
     * 获取映射值
     */
    String getMappingValue(String key) {
        return cacheMap.get(key);
    }

    private void loadMapping() {
        logger.info("开始加载redis映射关系！");
        Jedis jedis = null;
        try {
            //建立redis连接
            jedis = new Jedis(this.redisIp, this.redisPort, REDIS_TIMEOUT_MILLS);
            //查询映射，如果查询为空不替换
            Map<String, String> newMap = hgetAllMap(jedis);
            if (null != newMap && newMap.isEmpty()) {
                cacheMap.clear();
                cacheMap.putAll(newMap);
            } else {
                logger.warn("加载redis映射[{}]失败：查询为空不执行替换！", cacheKey);
            }
            logger.info("加载redis映射[{}]结果，映射值为[{}]", cacheKey, cacheMap.toString());
        } catch (Exception e) {
            logger.warn("加载redis映射发生异常，异常信息:" + e.getMessage(), e);
        } finally {
            //关闭连接
            closeRedis(jedis);
        }
    }

    /**
     * @param jedis
     * @return
     */
    private Map<String, String> hgetAllMap(Jedis jedis) {
        Map<String, String> newMap = jedis.hgetAll(cacheKey);
        if (null != newMap && !newMap.isEmpty()) {
            Iterator<String> it = newMap.keySet().iterator();
            while (it.hasNext()) {
                String key = it.next();
                String value = newMap.get(key);
                if (StringUtils.isBlank(key) || StringUtils.isBlank(value)) {
                    logger.warn("redis中key为[{}]的子键[{}]或子建对应的值[{}]为空", cacheKey, key, value);
                    newMap.remove(key);
                }
            }
        }
        return newMap;
    }

    @Override
    public void loadConfig() {
        this.redisIp = PropertyUtils.getProp(CONF_REDIS_YX_HOSTNAME);
        this.redisPort = Integer.parseInt(PropertyUtils.getProp(CONF_REDIS_YX_PORT));
    }

    private void closeRedis(Jedis jedis) {
        if (null != jedis) {
            try {
                jedis.close();
            } catch (Exception e) {
                logger.warn("关闭redis客户端发生异常，异常信息:" + e.getMessage());
            }
        }
    }
}
