package com.dragon.netty.utils;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;


public class URLCheckUtils {
    private final static Logger logger = LoggerFactory.getLogger(URLCheckUtils.class);

    //socket 超时时间
    private static final int SOCKET_TIMEOUT_MILLS = 5 * 1000;

    public static boolean isUrlAvailable(String url) {
        boolean rtn = false;
        try {
            String[] arr = url.split(":");
            rtn = isHostConnectable(arr[0], Integer.parseInt(arr[1]));
        } catch (Exception e) {
            logger.warn("change url to ip and port fail,msg:" + e.getMessage(), e);
        }
        return rtn;
    }

    /**
     * 判断ip，端口是否可用
     *
     * @param host ip
     * @param port port
     * @return
     */
    private static boolean isHostConnectable(String host, int port) {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), SOCKET_TIMEOUT_MILLS);
        } catch (Exception e) {
            logger.debug("check url connectable fail" + e.getMessage(), e);
            return false;
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                logger.warn("close socket fail" + e.getMessage(), e);
            }
        }
        return true;
    }
}
