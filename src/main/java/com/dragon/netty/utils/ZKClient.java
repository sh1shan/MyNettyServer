package com.dragon.netty.utils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;


public class ZKClient {
    private final static Logger logger = LoggerFactory.getLogger(ZKClient.class);

    private ZooKeeper zkClient;
    private CreateMode mode;
    private String zkAddress;
    private static final int DEF_SESSION_TIMEOUT_MILLS = 30000;

    public ZKClient(String string, Watcher watcher) throws IOException {
        this(string, CreateMode.PERSISTENT, watcher);
    }

    public ZKClient(String string, CreateMode mode, Watcher watcher) throws IOException {
        this.zkClient = new ZooKeeper(string, DEF_SESSION_TIMEOUT_MILLS, watcher);
        this.mode = mode;
        zkAddress = string;
    }

    public boolean reconnect(Watcher watcher) {
        try {
            zkClient.close();
        } catch (InterruptedException e) {

        }
        try {
            zkClient = new ZooKeeper(zkAddress, DEF_SESSION_TIMEOUT_MILLS, watcher);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean isAvailable() {
        ZooKeeper.States states = zkClient.getState();
        if (states != null) {
            return states.isConnected() && states.isAlive();
        }
        return false;
    }

    public void close() throws InterruptedException {
        this.zkClient.close();
    }

    public String registerService(String path, String date) {
        try {
            return this.zkClient.create(path, date.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
        } catch (KeeperException var4) {
            logger.error(var4.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    public boolean updateRegisterService(String path, String data) {
        try {
            this.zkClient.setData(path, data.getBytes(), -1);
            return true;
        } catch (KeeperException var4) {
            logger.error(var4.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    public boolean exist(String path) {
        try {
            Stat stat = this.zkClient.exists(path, true);
            if (stat == null) {
                return false;
            } else {
                return true;
            }
        } catch (KeeperException var4) {
            logger.error(var4.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    public void unregisterService(String path) {
        try {
            this.zkClient.delete(path, -1);
        } catch (KeeperException var3) {
            logger.error(var3.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
    }
    public List<String> listService(String path,boolean watch)throws Exception{
        List children = Collections.emptyList();
        children = this.zkClient.getChildren(path,watch);
        return children;
    }

    public String getServiceData(String path){
        try {
            Stat ke = new Stat();
            byte[] byteDate = this.zkClient.getData(path,true,ke);
            String data=new String(byteDate);
            return data;
        }catch (KeeperException var5){
            logger.error(var5.getMessage());
        }catch (InterruptedException e){
            logger.error(e.getMessage());
        }
        return null;
    }
}
