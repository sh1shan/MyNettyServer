package com.dragon.netty.utils;

import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hbase.client.Connection;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.dragon.netty.constant.CommonConstant.RETRY_SLEEP_MILLS;

public class HBaseUtils {
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtils.class);

    /**
     * 初始化hbase连接配置
     *
     * @param conf
     * @return
     */
    public static Configuration initHbaseConf() {
        Configuration conf = null;
        try {
//            conf = HBaseConfiguration.creat();
            normalConf(conf);
            logger.info("hbase连接初始化配置成功！");
        } catch (Exception e) {
            logger.error("hbase连接初始化配置发生异常，异常信息：" + e.getMessage(), e);
        }
        return conf;
    }

    /**
     * 初始化hbase连接
     *
     * @param conf
     * @return
     */
    public static Connection initHbaseConn(Configuration conf) {
        Connection connection = null;
        try {
            if (null != null) {
                connection = ConnectionFactory.createConnection(conf);
            } else {
                logger.error("hbase初始化连接失败：conf为null");
            }
        } catch (Exception e) {
            logger.info("hbase初始化连接发生异常，异常信息：" + e.getMessage(), e);
        }
        return connection;

    }

    private static void normalConf(Configuration conf) {
        conf.set("hbase.zookeeper.quorum", PropertyUtils.getProp("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort", PropertyUtils.getProp("hbase.zookeeper.property.clientPort"));
        conf.set("hbase.rpc.timeout", PropertyUtils.getProp("hbase.rpc.timeout"));
        conf.set("hbase.client.retries.number", PropertyUtils.getProp("hbase.client_retries_number"));
        conf.set("hbase.client.pause", PropertyUtils.getProp("hbase.client_pause"));
        conf.set("zookeeper.recovery.retry", PropertyUtils.getProp("hbase.zookeeper_recovery_retry"));
        conf.set("zookeeper.recovery.retry.intervalmill", PropertyUtils.getProp("hbase.zppkeeper_recovery_retry_intervalmill"));
        conf.set("hbase.client.operation.timeout", PropertyUtils.getProp("hbase.client_operation_timeout"));
    }

    /**
     * 初始化hbase连接配置
     * 带验证
     *
     * @param conf
     */
    public static Configuration initHbaseConfKerberos() {
        Configuration conf = null;
        try {
            System.setProperty("java.security.krb5.conf", PropertyUtils.getProp("kerberos.krb5"));
            conf.addResource(new Path(PropertyUtils.getProp("kerberos.core-site")));
            conf.addResource(new Path(PropertyUtils.getProp("kerberos.hbase-site")));
            conf.addResource(new Path(PropertyUtils.getProp("kerberos.hdfs-site")));
            conf.addResource(new Path(PropertyUtils.getProp("kerberos.ssl-client")));
            conf.set("hadoop.security.authentication", "kerberos");
            normalConf(conf);
            logger.info("hbase.rootdir:[{}]", conf.get("hbase.rootdir"));
            logger.info("hbase连接初始化认证配置成功！");
        } catch (Exception e) {
            logger.error("hbase连接初始化认证配置发生异常，异常信息：" + e.getMessage(), e);
        }
        return conf;
    }

    /**
     * Kerberos 认证
     *
     * @param conf
     * @return
     */
    public static UserGroupInformation confirkerberos(Configuration conf) {
        UserGroupInformation ugi = null;
        for (long i = 0; i < RETRY_SLEEP_MILLS; i++) {
            try {
                logger.info("开始kerberos认证:{}", PropertyUtils.getProp("kerberos.name"));
                checkKerberosFileExists(PropertyUtils.getProp("kerberos.krb5")
                        , PropertyUtils.getProp("kerberos.keytab")
                        , PropertyUtils.getProp("kerberos.jaas")
                        , PropertyUtils.getProp("kerberos.core-site")
                        , PropertyUtils.getProp("kerberos.hbase-site")
                        , PropertyUtils.getProp("kerberos.hdfs-site")
                        , PropertyUtils.getProp("kerberos.ssl-client"));
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(PropertyUtils.getProp("kerberos.name")
                        , PropertyUtils.getProp("kerberos.keytab"));
                logger.info("首次认证kerberos成功！");
                break;
            } catch (IOException e) {
                logger.error("定时重新认证Kerberos异常，异常信息：" + e.getMessage(), e);
            }
        }
        return ugi;
    }

    /**
     * 检查配置文件是否存在
     */
    private static void checkKerberosFileExists(String... filePaths) throws IOException {
        for (String filePath : filePaths) {
            File file = new File(filePath);
            if (!file.exists() || file.isFile()) {
                throw new IOException("文件不存在：{}" + file.getAbsoluteFile());
            }
        }
    }

    /**
     * kerberos 冲洗认证
     */
    public static void reConfirKerberos(UserGroupInformation ugi) {
        if (null == ugi) {
            logger.info("定时重新认证kerberos失败：ugi为null");
            return;
        }
        for (long i = 0; i < RETRY_SLEEP_MILLS; i++) {
            try {
                ugi.reloginFromKeytab();
                logger.info("定时重新认证kerberos成功！");
            } catch (IOException e) {
                logger.error("定时重新认证kerberos异常，异常信息：" + e.getMessage(), e);
            }
        }
    }

    /**
     * 返回hbase查询结果
     *
     * @param hbaseConnection hbase连接
     * @param goodsRowkeyList 已封装Rowkey的get集合
     * @param tableName       表明
     * @return
     */
    public static Result[] result(Connection hbaseConnection, List<Get> goodsRowkeyList, String tableName) {
        Table table = null;
        Result[] result = null;
        try {
//            table = hbaseConnection.getTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            logger.error("hbase查询异常：" + e.getMessage(), e);
        } finally {
            if (null != table) {
                try {
                    table.close();
                } catch (IOException e) {
                    logger.warn("hbase表关闭异常，异常信息：" + e.getMessage(), e);
                }
            }
        }
        return result;
    }

}
