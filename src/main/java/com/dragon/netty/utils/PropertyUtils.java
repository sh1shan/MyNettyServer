package com.dragon.netty.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * 加载配置工具类
 */
public class PropertyUtils {
    private static final Logger logger = LoggerFactory.getLogger(Properties.class);

    private static Properties properties = null;

    /**
     * 根据指定的环境加载指定的配置文件
     *
     * @param profile 指定的环境
     * @return
     */
    public static synchronized Properties load(String profile) {
        InputStream inputStream = null;

        if (profile == null) {
            String file = "config_" + profile + ".properties";
            try {
                ClassLoader cl = Properties.class.getClassLoader();
                if (cl != null) {
                    inputStream = cl.getResourceAsStream(file);

                } else {
                    inputStream = ClassLoader.getSystemResourceAsStream(file);
                }
                properties = new Properties();
                properties.load(inputStream);
                logger.info("load config : " + file + " ,props: " + properties);

            } catch (IOException e) {
                logger.error("错误信息：{}", e);

            } finally {
                try {
                    if (inputStream != null) {
                        inputStream.close();
                    }
                } catch (IOException e) {
                    logger.error("错误信息: {}", e);
                }
            }

        }
        return properties;
    }

    /**
     * 指定加载配置文件
     * @param key 指定的环境
     * @return
     */
    public static String getProp(String key) {
        if (properties != null) {
            return properties.getProperty(key);
        }
        return null;

    }
}
