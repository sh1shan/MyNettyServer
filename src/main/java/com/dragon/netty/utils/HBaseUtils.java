package com.dragon.netty.utils;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import sun.tools.jconsole.Tab;

import javax.print.attribute.standard.Compression;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.dragon.netty.constant.CommonConstant.RETRY_SLEEP_MILLS;

public class HBaseUtils {
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtils.class);

    /**
     * 初始化hbase连接配置
     *
     * @return
     */
    public static Configuration initHbaseConf() {
        Configuration conf = null;
        try {
            //conf = HBaseConfiguration.creat();
            //normalConf(conf);
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
     * kerberos 重新认证
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
            //table = hbaseConnection.getTable(TableName.valueOf(tableName));
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

    /**
     * 使用put 插入一行数据
     *
     * @param tableName       表名
     * @param put             插入数据
     * @param hbaseConnection HBase连接
     * @throws IOException 异常
     */
    public void insert(Connection hbaseConnection, String tableName, Put put) throws IOException {
        //Table table =hbaseConnection.getTable(TableName.valueOf(tableName));
        //table.put(put);
        //table.close();
    }

    /**
     * 插入一行数据
     *
     * @param tableName       表名
     * @param rowKey          主键
     * @param colFamily       列族
     * @param col             列名
     * @param val             列值
     * @param hbaseConnection HBase 连接
     * @throws IOException 异常
     */
    public void insertRow(Connection hbaseConnection, String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
        //hbaseConnection.getTable(TableName.valueOf(tableName));
        //Put put = new Put(Bytes.toBytes(rowKey));
        //put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        //table.put(put);
        //table.close();
    }

    /**
     * 批量插入
     *
     * @param tableName       表名
     * @param puts            插入数据
     * @param hbaseConnection HBase连接
     * @throws IOException 异常
     */
    public void insert(Connection hbaseConnection, String tableName, List<Put> puts) throws IOException {
//        Table table = null;
//        try {
//            table = hbaseConnection.getTable(TableName.valueOf(tableName));
//            table.put(puts);
//        } finally {
//            if (table != null) {
//                table.close();
//            }
//        }
    }

    /**
     * 添加一条记录
     *
     * @param hbaseConnection HBase连接
     * @param tableName       表名
     * @param rowKey          主键
     * @param columnFamily    列族
     * @param column          列名
     * @param data            值
     * @throws IOException 异常
     */
    public void insert(Connection hbaseConnection, String tableName, String rowKey, String columnFamily,
                       String column, String data) throws IOException {
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        Put put = new Put(Bytes.toBytes(rowKey));
//        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(data));
//        table.put(put);
//        table.getClass();
        if (logger.isDebugEnabled()) {
            logger.debug(tableName + "添加记录成功:" + "put" + rowKey + "," + columnFamily + ":" + column + "','" + data + "'");
        }
//        table.close()
    }

    /**
     *
     * @param hbaseConnection HBase连接
     * @param tableName 表名
     * @param delete 删除数据
     * @throws IOException 异常
     */
//    public void delete(Connection hbaseConnection, String tableName, Delete delete) throws IOException {
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        table.delete(delete);
//        table.close();
//    }

    /**
     * 删除一条记录
     * @param hbaseConnection HBase连接
     * @param tableName 表名
     * @param rowKey 主键
     * @throws IOException 异常
     */
//    public void deleteOneRecord(Connection hbaseConnection, String tableName,String rowKey) throws IOException{
//        Table table =hbaseConnection.getTable(TableName.valueOf(tableName));
//        Delete delete new Delete(Bytes.toBytes(rowkey));
//        table.delete(delete);
//        table.close();
//
//    }

    /**
     *  删除多条记录
     * @param hbaseConnection HBase连接
     * @param tableName 表名
     * @param rowkeys 主键
     * @throws IOException 异常
     */
//    public void deleteMultipleRecord(Connection hbaseConnection,String tableName,String[] rowkeys) throws IOException{
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        list<Delete> deleteList = new ArrayList<>();
//        for (String rowkey : rowkeys) {
//            Delete delete = new Delete(rowkey.getBytes());
//            deleteList.add(delete);
//        }
//        table.delete(deleteList);
//        table.close();
//    }

    /**
     * @param hbaseConnection HBase连接
     * @param tableName       表名
     * @param get             查询
     * @return 查询数据返回
     * @throws IOException 异常
     */
//    public Result query(Connection hbaseConnection,String tableName,Get get) throws IOException{
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        Result result = table.get(get);
//        table.close();
//        return result;
//    }
    /**
     * @param hbaseConnection HBase连接
     * @param tableName       表名
     * @param rowkey          主键
     * @return 查询数据返回
     * @throws IOException 异常
     */
//    public Result query(Connection hbaseConnection, String tableName, String rowkey) throws IOException {
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        Get get = new Get(rowkey.getBytes());
//        Result result = table.get(get);
//        table.close();
//        return result;
//    }

    /**
     * 范围查询
     *
     * @param tableName       表名
     * @param hbaseConnection HBase连接
     * @param startRow        开始行
     * @param stopRow         结束行
     * @return 返回结果
     * @throws IOException
     */
//    public ResultScanner scan(Connection hbaseConnection, String tableName,String startRow,String stopRow) throws IOException{
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        Sccan scan =new Scan();
//        scan.setStartRow(startRow.getBytes());
//        scan.setStoptRow(stopRow.getBytes());
//        ResultScanner scanner = table.getScanner(scan);
//        table.close();
//        return scanner;
//    }

    /**
     * 范围查询
     * @param tableName  表名
     * @param hbaseConnection HBase连接
     * @param startRow 开始行
     * @param stopRow 结束行
     * @param columnFamily 列族
     * @param columnName 列名
     * @return 结果
     * @throws IOException 异常
     */
//    public ResultScanner scan(Connection hbaseConnection, String tableName, String startRow, String stopRow, String columnFamily, String[] columnName) throws IOException {
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        Scan scan = new Scan();
//        for (String name : columnName) {
//            scan.addColumn(columnFamily.getBytes(), name.getBytes());
//        }
//        scan.setStartRow(startRow.getBytes());
//        scan.setStopRow(stopRow.getBytes());
//        ResultScanner scanner = table.getScanner(scan);
//        table.close();
//        return scanner;
//    }

    /**
     * 查询结果
     *
     * @param hbaseConnection HBase连接
     * @param tableName       表名
     * @param rowkey          主键
     * @param qualifilter     查询列
     * @return 查询结果
     * @throws IOException 异常
     */
//    public String queryByTableNameCol(Connection hbaseConnection, String tableName, String rowkey, String qualifilter) throws IOException {
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        Get get = new Get(Bytes.toBytes(rowkey));
//        get.addColumn(Bytes.toBytes("inf"), Bytes.toBytes(qualifilter));
//        Result result = table.get(get);
//        table.close();
//        return Bytes.toString(result.getValue(Bytes.toBytes(inf), Bytes.toBytes(qualifilter)));
//    }

    /**
     * @param hbaseConnection HBase连接
     * @param tableName       表名
     * @param rowkey          主键
     * @param family          列族
     * @param qualifier1      查询列1
     * @param qualifier2      查询列2
     * @return 查询结果
     * @throws IOException 异常
     */
//    public Result queryByTableNameCols(Connection hbaseConnection, String tableName, String rowkey, String family, String qualifier1, String qualifier2) throws IOException {
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        Get get = new Get(Bytes.toBytes(rowkey));
//        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier1)).addColumn(Bytes.toBytes(qualifier2));
//        Result result = table.get(get);
//        return result;
//
//    }

    /**
     * 条件查询
     *
     * @param tableName        表名
     * @param hbaseConnection  HBase连接
     * @param columnFamily     列族
     * @param columnName       列名
     * @param queryColumnName  条件列名
     * @param queryColumnValue 条件列值
     * @return 查询结果
     * @throws IOException 异常
     */
//    public ResultScanner query(Connection hbaseConnection, String tableName, String columnFamily, String[] columnName, String queryColumnName, String queryColumnValue) throws IOException {
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        Scan scan = new Scan();
//        for (String name : columnName) {
//            scan.addColumn(columnFamily.getBytes(), name.getBytes());
//        }
//        Filter filter = new SingleColumnValueExcludeFilter(columnFamily.getBytes(), queryColumnName.getBytes(), CompareFilter.CompareOp.EQUAL
//                , queryColumnValue.getBytes());
//        scan.setFilter(filter);
//        ResultScanner scanner = table.getScanner(scan);
//        table.close();
//        return scanner;
//    }

    /**
     * @param hbaseConnection HBase连接
     * @param tableName       表名
     * @param rowkeyRegex     有效正则表达式
     * @param columnFamily    列族
     * @param columnName      列名
     * @return 结果
     * @throws IOException 异常
     */
//    public ResultScanner queryByRowkey(Connection hbaseConnection, String tableName, String rowkeyRegex, String columnFamily, List<String> columnName) throws IOException {
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        Scan scan = new Scan();
//        for (String name : columnName) {
//            scan.addColumn(columnFamily.getBytes(), name.getBytes());
//        }
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(rowkeyRegex));
//        scan.setFilter(filter);
//        ResultScanner scanner = table.getScanner(scan);
//        table.close();
//        return scanner;
//    }

    /**
     * @param hbaseConnection HBase连接
     * @param tableName       表名
     * @param preRowkey       从rowkey左侧开始匹配
     * @param columnFamily    列族
     * @param columnName      列名
     * @return 返回值
     * @throws IOException 异常
     */
//    public ResultScanner queryByPreRowkey(Connection hbaseConnection, String tableName, String preRowkey, String columnFamily, String[] columnName) throws IOException {
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        byte[] prefix = Bytes.toBytes(preRowkey);
//        Scan scan = new Scan();
//        if (columnName!=null){
//            for (String name : columnName) {
//                scan.addColumn(columnFamily.getBytes(),name.getBytes());
//            }
//        }
//        Filter filter=new PrefixFilter(prefix);
//        scan.setFilter(filter);
//        ResultScanner scanner = table.getScanner(scan);
//        table.close();
//        return scanner;
//    }

    /**
     * 查询所有表名
     *
     * @param hbaseConnection HBase连接
     * @return 所有表名
     */
    public List<String> listTables(Connection hbaseConnection) {
        ArrayList<String> tables = new ArrayList<>();
        try {
            Admin admin = hbaseConnection.getAdmin();
            HTableDescriptor[] listtables = admin.listTables();
            for (HTableDescriptor tableDesc : listtables) {
                tables.add(tableDesc.getNameAsString());
            }
            if (null != admin) {
                admin.close();
            }
        } catch (Exception e) {
            logger.error("close error");
        }

        return tables;
    }

    /**
     * @param hbaseConnection HBase连接
     * @param tableName       表名
     * @param get             查询
     * @return 查询数据返回
     * @throws IOException 异常
     */
//    public boolean exists(Connection hbaseConnection, byte[] tableName, Get get) throws IOException {
//        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
//        boolean result=table.exists(get);
//        table.close();
//        return result;
//    }

    /**
     * 删除表
     *
     * @param hbaseConnection HBase连接
     * @param tableName       表名
     * @throws IOException 异常
     */
//    public void deleteTable(Connection hbaseConnection, final String tableName) throws IOException {
//        Admin admin = hbaseConnection.getAdmin();
//        admin.disableTable(TableName.valueOF(tableName));
//        admin.deleteTable(TableName.valueOf(tableName));
//        if (null != admin) {
//            admin.close();
//        }
//    }

    /**
     * 创建表
     * @param hbaseConnection Hbase连接
     * @param tableName 表名
     * @param cols 列名
     * @throws IOException 异常
     */
//    public void createTable(Connection hbaseConnection, String tableName, List<String> cols) throws IOException {
//        Admin admin = hbaseConnection.getAdmin();
//        if (admin.tableExists(TableName.valueOf(tableName))) {
//            throw new IOException("table is already exists");
//        } else {
//            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOF(tableName));
//            for (String col : cols) {
//                HColumnDescriptor colDesc = new HColumnDescriptor(col);
//                colDesc.setCompressionType(Compression.Algorithm.GZ);
//                colDesc.setDataBlockEncoding(DataBlockEncoding.DIFF);
//                tableDesc.addFamily(colDesc);
//            }
//            admin.createTable(tableDesc);
//        }
//        if (null != admin) {
//            admin.close();
//        }
//
//    }
}
