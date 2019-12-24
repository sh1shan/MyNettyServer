package com.dragon.netty.constant;


/**
 * 通用常量接口
 */
public interface CommonConstant {
    //redis中的key
    String REDIS_KEY_TABLE_MAPPING_NAME = "hello";

    //redis ip
    String CONF_REDIS_YX_HOSTNAME = "redis.yx.host";

    //redis单机端口
    String CONF_REDIS_YX_PORT = "redis.yx.port";

    //zk连接地址
    String CONF_ZOOKEEPER_ADDRESS = "zookeeper.address";

    //RETRY最大次数
    long RETRY_MAX_TIME = 3;

    //RETRY间隔时间
    long RETRY_SLEEP_MILLS = 3;

    //通用等待时间
    long COMM_SLEEP_MILLS = 5 * 60 * 1000;
}
