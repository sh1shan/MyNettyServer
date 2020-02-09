package com.dragon.netty.dispatcher;

import com.dragon.netty.helper.CommonService;
import com.dragon.netty.helper.interfaces.IInitable;
import jrx.anyest.netty.http.HttpRequestRouter;
import jrx.anyest.netty.http.IRequestDispater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dragon.netty.constant.NettyConstant.REGISTER_PATH_01;
import static com.dragon.netty.constant.NettyConstant.REQUEST_PATH_01;


public class DispatcherService implements IInitable {
    private static final Logger logger = LoggerFactory.getLogger(DispatcherService.class);
    private final CommonService commonService;
    private final HttpRequestRouter router;

    public DispatcherService(CommonService _commonService, HttpRequestRouter _router) {
        this.commonService = _commonService;
        this.router = _router;
    }

    @Override
    public void init() {
        initDispatchers();
    }

    //初始化分发器
    private void initDispatchers() {
        //这里导入接口
        addDispatcher(new BDRM29(commonService,REGISTER_PATH_01,REQUEST_PATH_01));
    }

    private void addDispatcher(IRequestDispatcher dispatcher) {
        if (null != router && null != commonService && null != dispatcher) {
            router.addDispatcher(dispatcher);
            logger.info("初始化[{}]成功！", dispatcher.getClass().getSimpleName());
        } else {
            logger.info("初始化dispatcher失败：HttpRequestRouter或CommonService或Dispatcher为null!");
        }
    }
}
