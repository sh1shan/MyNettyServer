package com.dragon.netty.helper.watchers;

import com.dragon.netty.helper.interfaces.IAnswerService;
import com.dragon.netty.helper.interfaces.IWatchable;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkPathWatcher implements Watcher, IWatchable {

    private static final Logger logger = LoggerFactory.getLogger(ZkPathWatcher.class);

    //监听路径
    private final String watchPath;

    //执行动作对象
    private final IAnswerService helper;

    public ZkPathWatcher(IAnswerService _helper, String _watchPath) {
        this.helper = _helper;
        this.watchPath = _watchPath;

    }

    @Override
    public void watchAndNotify() {
        this.helper.answerAndOperate(this);
    }

    /**
     * zk监听器执行的方法
     *
     * @param watchedEvent
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info("notice:address in the public [{}] was change", watchPath);
        watchAndNotify();
    }
}
