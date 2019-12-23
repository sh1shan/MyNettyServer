package com.dragon.netty.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dragon.netty.helper.CommonService;
import com.dragon.netty.helper.interfaces.ICloseable;
import io.netty.handler.codec.http.HttpRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.dragon.netty.utils.LogShield.logOperate;

/**
 * 通用工具类
 */

public class CommonUtils {
    private static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

    private static final String SUSS = "0000";
    private static final String HBaseERROR = "4001";
    private static final String PARMERROR = "4003";


    /**
     * 解析URL
     *
     * @param url
     * @return paramMap
     */
    public static Map<String, String> analysisUrl(String url) {
        HashMap<String, String> paramMap = new HashMap<>();
        if (StringUtils.isEmpty(url)) {
            logger.warn("url is null");
            return paramMap;
        }

        try {
            String[] args = url.substring(url.indexOf("?") + 1).split("&");
            for (String arg : args) {
                String strKey = arg.substring(0, arg.indexOf("="));
                String strValue = arg.substring(0, arg.indexOf("=") + 1);
                paramMap.put(strKey, strValue);
                //通过浏览器http地址栏传递后来看到的要解码
                if (strKey.equals("name")) {
                    paramMap.put(strKey, URLDecoder.decode(strValue, "utf-8"));
                }
                if (strKey.equals("svc_type")) {
                    paramMap.put(strKey, URLDecoder.decode(strValue, "utf-8"));
                }
                if (strKey.equals("crName")) {
                    paramMap.put(strKey, URLDecoder.decode(strValue, "utf-8"));
                }
            }
            return paramMap;
        } catch (Exception e) {
            logger.warn("can not analysis url");
            return paramMap;
        }
    }


    /**
     * 必须参数为空
     *
     * @param param
     * @return resp
     */
    public static JSONObject mustParamNotExistReturn(String param) {
        JSONObject resp = new JSONObject();
        logger.info("调用方法请求参数{}不存在或者为空", param);
        resp.put("result", PARMERROR);
        resp.put("msg", String.format("调用方法请求参数%s不存在或者为空", param));
        resp.put("respTime", System.currentTimeMillis());
        return resp;
    }

    /**
     * 添加上送决策必输字段
     *
     * @param paramMap
     */
    public static void enrichParam(Map<String, String> paramMap) {
        String sessionNbr = UUID.randomUUID().toString().replace("-", "");
        paramMap.put("busId", "0000");
        paramMap.put("branchId", "0000");
        paramMap.put("agencyId", "gfyhpoc");
        paramMap.put("evenType", "OTHER");
        // ActiveId 已作校验
        paramMap.put("evenId", paramMap.get("Activeid"));
        paramMap.put("sessionNbr", sessionNbr);
        paramMap.put("eventStartDate", System.currentTimeMillis() + "");

    }

    /**
     * 获取HttpSenderClient失败
     *
     * @return resp
     */
    public static JSONObject nullClient() {
        JSONObject resp = new JSONObject();
        resp.put("result", "3003");
        resp.put("msg", "获取HttpSenderClient失败");
        resp.put("respTime", System.currentTimeMillis());
        return resp;
    }

    /**
     * 决策结果
     *
     * @param result
     * @param timeOut
     * @param policyTime
     * @return
     */
    public static JSONObject returnResult(String result, long timeOut, long policyTime) {
        JSONObject resp = new JSONObject();
        if (null == result) {
            if (policyTime > timeOut) {
                resp.put("result", "3001");
                resp.put("msg", "请求storm决策超时");
                resp.put("respTime", System.currentTimeMillis());
                logger.info("请求storm决策超时，timeout:{},stormConsume:{}", timeOut, policyTime);
            } else {
                resp.put("msg", String.format("请求Storm决策异常，timeOut：%d，stormConsume：%d", timeOut, policyTime));
                logger.info("请求Storm决策异常，timeOut{},stormConsume:{}", timeOut, policyTime);
            }
        } else {
            resp = JSONObject.parseObject(result);
        }
        return resp;
    }

    /**
     * 公用请求storm方法
     *
     * @param request
     * @param mustField
     * @param shouldField
     * @param policyTime
     * @return
     */
    public static JSONObject policy(HttpRequest request, List<String> mustField, List<String> shouldField, CommonService commonService, String requestPath, long timeout,long policyTime) {
        JSONObject resp;
        String uri = URLDecoder.decode(request.getUri());
        logger.info("接收到的uri为{}", logOperate(uri));
        Map<String, String> paramMap = analysisUrl(uri);
        if (null == paramMap || paramMap.isEmpty()) {
            logger.warn("参数异常,请求结束");
            return null;
        }
        //非空判断
        for (String param : mustField) {
            //必输字段
            String value = paramMap.get(param);
            if (StringUtils.isEmpty(value)) {
                resp = mustParamNotExistReturn(value);
                return resp;
            }
        }
        //非必填字段判断
        for (String param : shouldField) {
            String value = paramMap.get(param);
            if (StringUtils.isEmpty(value)) {
                paramMap.put(param, "");
            }
        }

        //添加必送字段
        enrichParam(paramMap);
        JSONObject sendMessage = new JSONObject();
        for (String param : paramMap.keySet()) {
            sendMessage.put(param, paramMap.get(param));
        }
        long stormStart = System.currentTimeMillis();
        String url = getRequestUrl(commonService, requestPath);
        String result = commonService.getPostResponse(url, sendMessage.toString());
        if (logger.isDebugEnabled()) {
            logger.debug("请求Storm消耗的时间【{}】ms", policyTime);
            logger.debug("2.决策返回的数据：{}", result);
        }
        resp = returnResult(result, timeout, policyTime);
        return resp;
    }

    /**
     * 组装请求地址
     *
     * @param commonService
     * @param requestPath   必须带
     * @return
     */
    public static String getRequestUrl(CommonService commonService, String requestPath) {
        String topoUrl = commonService.getTopoUrl();
        if (StringUtils.isNotBlank(topoUrl)) {
            return "http://" + topoUrl + requestPath;
        } else {
            return StringUtils.EMPTY;
        }
    }

    /**
     * 请求storm消耗时间
     *
     * @param stormStart
     */
    public static void stormTime(Long stormStart) {
        logger.info("请求storm消耗时间:{}ms", System.currentTimeMillis());
    }



    /**
     * HBase 请求参数校验
     *
     * @param paramMap  传入参数列表
     * @param mustField 必要参数
     * @return
     */
    public static JSONObject policyHBase(Map<String, String> paramMap, List<String> mustField) {
        JSONObject resp = new JSONObject();
        if (null == paramMap || paramMap.isEmpty()) {
            logger.warn("参数异常有误，请求结束");
            resp = paramNotExistReturn("params");
            return resp;

        }
        //非空判断
        for (String param : mustField) {
            //必输字段
            String value = paramMap.get(param);
            if (value.isEmpty()) {
                resp = paramNotExistReturn(param);
                return resp;
            }
        }
        return resp;
    }

    /**
     * 参数不存在，json报文封装
     *
     * @param param
     * @return
     */
    public static JSONObject paramNotExistReturn(String param) {
        JSONObject resp = new JSONObject();
        logger.info("调用请求参数{}不存在或者为空，请检查", param);
        resp.put("result_code", PARMERROR);
        resp.put("result_desc", String.format("调用请求参数%s不存在或者为空,请检查", param));
        resp.put("respTime", System.currentTimeMillis());
        return resp;

    }

    /**
     * rowKey的倒装
     *
     * @param goods
     * @return
     */
    public static String getRowkeyReverse(String goods) {
        int goodLength = goods.replace(" ", "").length();
        String last = goods.replace(" ", "").substring(goodLength - 4, goodLength);
        String other = goods.replace(" ", "").substring(0, goodLength - 4);
        String rowKey = last + other;
        return rowKey;

    }

    /**
     * 获取参考时间距离现在的毫秒数
     *
     * @param referMills
     * @return
     */
    public static long awayCurrentMills(long referMills) {
        return System.currentTimeMillis() - referMills;
    }

    /**
     * 关闭多个closeable接口实现类对象
     * @param closers
     */
    public static void closees(ICloseable... closers) {
        Arrays.stream(closers).forEach(closer->{
            if (closer != null) {
                String name = closers.getClass().getSimpleName();
                try {
                    closer.close();
                } catch (Exception e) {
                    logger.info("[{}]关闭发生异常，异常信息：{}", name, e.getMessage(), e);
                }
            }
        });
    }
}
