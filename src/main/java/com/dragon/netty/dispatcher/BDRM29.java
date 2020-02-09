package com.dragon.netty.dispatcher;

import com.dragon.netty.helper.CommonService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 这个文件夹下是对应的处理逻辑，每个接口可以注册到netty服务中，这里有一个例子的接口
 */
public class BDRM29 implements IRequestDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(BDRM29.class);
    private final CommonService commonService;
    private final String registerPath;

    public BDRM29(CommonService _commonService, String _registerPath) {
        this.commonService = _commonService;
        this.registerPath = _registerPath;
    }
}
