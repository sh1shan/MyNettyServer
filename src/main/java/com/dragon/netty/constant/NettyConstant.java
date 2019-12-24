package com.dragon.netty.constant;


/**
 * Netty 常量接口
 */
public interface NettyConstant {

    //拓扑注册zk路径
    String TOPO_REGISTER_ZK_PATH="/hello/world";

    //注册到netty地址<不能/>
    String REGISTER_PATH_01="001";
    String REGISTER_PATH_02="002";

    //分发器请求拓扑url地址<必须带/>
    String REQUEST_PATH_01="/01";
    String REQUEST_PATH_02="/02";
}
