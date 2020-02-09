package com.dragon.netty.start;


import com.dragon.netty.dispatcher.DispatcherService;
import com.dragon.netty.helper.CommonService;
import com.dragon.netty.utils.PropertyUtils;
import io.netty.util.internal.StringUtil;
//import jrx.anyest.netty.NettyService;
//import jrx.anyest.netty.http.HttpRequestRouter;
//import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 启停类
 * args[0]为环境参数
 */
public class NettyServerStart {
    private static final Logger logger = LoggerFactory.getLogger(NettyServerStart.class);
    //环境信息
    private static volatile String env = "prod";

    //nettyServer配置
    private static volatile int staryPort = 8080;
    private static volatile int bossThread = 2;
    private static volatile int workerThread = 8;
    private static volatile int processThread = 32;
    private static volatile int heartbeatSec = 5;
    private static volatile int soBacklog = 1024;
    private static volatile int portSize = 5;

    //通用服务
    private static volatile CommonService commonService;

    //dispatcher添加服务
    private static volatile DispatcherService dispatcherService;

    //路由器
    private static volatile HttpRequestRouter router;

    //netty服务
    private static volatile NettyServer nettyServer;

    /**
     * 启动方法
     */
    public static void main(String[] args) {
        logger.info("应用初始化");
        //初始化环境信息
        setEnvironment(args);
        //启动并根据启动状态作出动作
        if (initWithStatus()) {
            logger.info("应用启动成功！");
            addCloseHook();
        } else {
            logger.error("应用启动失败！");
            close();
        }
    }

    /**
     * 关闭方法
     */
    private static void close() {
        //关闭commonService
        if (null != commonService) {
            try {
                commonService.close();
            } catch (Exception e) {
                logger.warn("应用通用服务关闭异常！，异常信息：{}", e.getMessage(), e);
            }
        } else {
            logger.warn("应用通用服务关闭失败:commonService为null!");
        }
        if (null != nettyServer) {
            try {
                nettyServer.stop();
                logger.info("应用停止nettyServer成功！");
            } catch (Exception e) {
                logger.warn("应用停止nettyServer失败，异常信息：{}", e.getMessage(), e);
            }
        } else {
            logger.warn("应用停止nettyServer失败,nettyServer为null！");
        }
        logger.info("应用关闭结束！");
    }

    /**
     * 添加钩子
     * 程序异常及时释放
     */
    private static void addCloseHook() {
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> close())
        );
    }

    /**
     * 启动程序
     * 返回状态
     *
     * @return
     */
    private static boolean initWithStatus() {
        boolean isStarted = true;
        try {
            initConf();
            initDispatchers();
            initNettyServer();
        } catch (Exception e) {
            isStarted = false;
            logger.error("应用启动异常，异常信息:{}", e.getMessage(), e);
        }
        return isStarted;
    }

    /**
     *
     */
    private static void initNettyServer() throws Exception {
        nettyServer = NettyServer.build(staryPort)
                .bossThread(bossThread)
                .workerThread(workerThread)
                .processThread(processThread)
                .heartbeatSec(heartbeatSec)
                .soBacklog(soBacklog)
                .portSize(portSize)
                .httpRequestRouter(router);
        //启动服务，抛异常！
        nettyServer.start();
    }

    /**
     * 初始化dispatcher
     */
    private static void initDispatchers() {
        commonService = new CommonService();
        commonService.init();
        router = new HttpRequestRouter();
        dispatcherService = new DispatcherService(commonService, router);
        dispatcherService.init();
    }

    /**
     * 初始化配置文件
     */
    private static void initConf() {
        PropertyUtils.load(env);
        staryPort = Integer.parseInt(PropertyUtils.getProp("startPort"));
        bossThread = Integer.parseInt(PropertyUtils.getProp("bossThread"));
        workerThread = Integer.parseInt(PropertyUtils.getProp("workerThread"));
        processThread = Integer.parseInt(PropertyUtils.getProp("processThread"));
        heartbeatSec = Integer.parseInt(PropertyUtils.getProp("heartbeatSec"));
        soBacklog = Integer.parseInt(PropertyUtils.getProp("soBacklog"));
        portSize = Integer.parseInt(PropertyUtils.getProp("portSize"));
    }

    private static void setEnvironment(String[] args) throws NullPointerException {
        if (null != args && args.length > 0 && StringUtil.isNotBlank(args[0])) {
            env = args[0];
            logger.info("应用启动环境[{}]", env);
        } else {
            throw new NullPointerException("应用启动失败，启动参数为空！");
        }
    }

}
