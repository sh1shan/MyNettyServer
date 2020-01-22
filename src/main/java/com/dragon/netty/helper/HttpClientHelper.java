package com.dragon.netty.helper;

import com.dragon.netty.helper.interfaces.ICloseable;
import com.dragon.netty.helper.interfaces.IInitable;
import com.dragon.netty.helper.interfaces.Iloadable;
import com.dragon.netty.utils.PropertyUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class HttpClientHelper implements IInitable, Iloadable, ICloseable {
    private static final Logger logger = LoggerFactory.getLogger(HttpClientHelper.class);

    //编码格式
    private static final String CHARTSET_UTF8 = "UTF-8";
    //是否重试
    private static final boolean RETRY_ENABLE = false;
    //重试次数
    private static final int RETRY_TIMES = 3;
    //配置
    private int maxTotal = 1000;
    private int maxRoute = 1000;
    private int maxIdle = 3000;
    private int readTimeout = 5000;
    private int connectTimeout = 5000;

    //初始化内容
    private RequestConfig requestConfig;
    private SocketConfig socketConfig;
    private PoolingHttpClientConnectionManager manager;
    private ConnectionKeepAliveStrategy keepAliveStrategy;
    private CloseableHttpClient httpClient;


    @Override
    public void close() {
        if (null != httpClient) {
            try {
                httpClient.close();
                logger.info("httpClient关闭");
            } catch (IOException e) {
                logger.warn("httpClient关闭异常，异常信息:" + e.getMessage(), e);
            }
        } else {
            logger.warn("httpClient关闭失败：httpClient为null!");
        }
    }

    /**
     * 初始化
     * 注意：启动顺序不能乱；
     */
    @Override
    public void init() {
        loadConfig();
        initRequestCfig();
        iniSocketCfig();
        initManager();
        initKeepAliveStrategy();
        initHttpClient();
    }

    private void initHttpClient() {
        this.httpClient = HttpClients.custom()
                .setConnectionManager(manager)
                .setDefaultRequestConfig(requestConfig)
                .setDefaultSocketConfig(socketConfig)
                //设施是否重试
                .setRetryHandler(new DefaultHttpRequestRetryHandler(RETRY_TIMES, RETRY_ENABLE))
                .setKeepAliveStrategy(keepAliveStrategy)
                //设置定义清理默认10s
                .evictExpiredConnections()
                .evictIdleConnections(maxIdle, TimeUnit.MILLISECONDS)
                .build();
    }

    private void initKeepAliveStrategy() {
        this.keepAliveStrategy = new DefaultConnectionKeepAliveStrategy() {
            @Override
            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                //1.默认存活策略
                long aliveTime = super.getKeepAliveDuration(response, context);
                //2.自定义存活策略
                if (-1L == aliveTime) {
                    //判断head里是否有存活策略参数
                    String keepaliveval = null;
                    String alivetimeval = null;
                    //由于netty与http的报文不一致采用netty报文头重新定义存活策略
                    for (Header header : response.getAllHeaders()) {
                        if ("connect".equalsIgnoreCase(header.getName())) {
                            keepaliveval = header.getValue();
                        }
                        if ("alivetime".equalsIgnoreCase(header.getName())) {
                            alivetimeval = header.getValue();
                        }
                        if (null != keepaliveval && null != alivetimeval) {
                            break;
                        }
                    }
                    if (null != alivetimeval && "break-alive".equalsIgnoreCase(keepaliveval)) {
                        try {
                            //alive时间等于服务端设置时间与idle最大时间：服务端存活参数以s为单位
                            aliveTime = Math.max(Integer.valueOf(alivetimeval) * 1000, maxIdle);
                        } catch (Exception e) {
                            aliveTime = maxIdle;
                        }
                    }
                }
                //返回存活时间
                logger.debug("aliveTime:" + aliveTime);
                return aliveTime;
            }
        };
    }

    /**
     * 获取超时时间
     *
     * @return
     */
    int getReadTimeout() {
        return readTimeout;
    }

    /**
     * 请求响应
     * post方法
     *
     * @param url
     * @param data
     */
    String getReponse(String url, String data) {
        String res = null;
        HttpEntity entity2 = null;
        CloseableHttpResponse resp2 = null;
        HttpPost httpPost = null;
        try {
            httpPost = httpPost(url, data);
            resp2 = httpClient.execute(httpPost);
            entity2 = resp2.getEntity();
            res = EntityUtils.toString(entity2, CHARTSET_UTF8);
        } catch (IOException e) {
            logger.error("Http post error,msg:" + e.getMessage(), e);
        } finally {
            try {
                EntityUtils.consume(entity2);
                if (resp2 != null) {
                    resp2.close();
                }
            } catch (IOException e) {
                logger.warn("关闭响应IO异常，错误信息:{}", e);
            }
        }
        if (res != null) {
            if (resp2 != null) {
                logger.warn("响应结果极解析误，responseCode:{},uri:{}", resp2.getStatusLine().getStatusCode(), httpPost.getURI());
            } else {
                logger.warn("响应结果为null，respCode:{},uri:{}", resp2, httpPost.getURI());
            }
        }
        return res;
    }

    private HttpPost httpPost(String url, String body) throws IOException {
        HttpPost httpPost = new HttpPost(url);
        ByteArrayEntity entity = new ByteArrayEntity(body.getBytes(CHARTSET_UTF8));
        httpPost.addHeader(HTTP.CONTENT_TYPE, "application/json");
        httpPost.addHeader(HTTP.CONTENT_ENCODING, CHARTSET_UTF8);
        httpPost.addHeader(HttpHeaders.CONNECTION, HTTP.CONN_KEEP_ALIVE);
        entity.setContentType("text/json");
        entity.setContentEncoding(CHARTSET_UTF8);
        httpPost.setEntity(entity);
        return httpPost;
    }

    private void initManager() {
        this.manager = new PoolingHttpClientConnectionManager();
        // 设置连接池线程最大数量
        this.manager.setMaxTotal(maxTotal);
        // 设置单个路由最大的连接线程数量
        this.manager.setDefaultMaxPerRoute(maxRoute);

    }

    private void iniSocketCfig() {
        this.socketConfig = SocketConfig.custom().setSoTimeout(readTimeout)
                .setSoKeepAlive(true)
                .setTcpNoDelay(true)
                .build();
    }

    private void initRequestCfig() {
        this.requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout).setConnectionRequestTimeout(connectTimeout)
                .setSocketTimeout(readTimeout)
                .build();
    }

    @Override
    public void loadConfig() {
        this.maxTotal = Integer.parseInt(PropertyUtils.getProp("xxxx"));
        this.maxRoute = Integer.parseInt(PropertyUtils.getProp("xxxx"));
        this.maxIdle = Integer.parseInt(PropertyUtils.getProp("xxxx"));
        this.readTimeout = Integer.parseInt(PropertyUtils.getProp("xxxx"));


    }
}
