package com.dragon.netty.helper.interfaces;

/**
 * 响应接口
 * 与IWatchable配置使用
 */
public interface IAnswerService {
    /**
     * 初始化watchers
     */
    void initWatchers();

    /**
     * 响应方法
     * @param watchable
     */
   void answerAndOperate(IWatchable watchable);

}
