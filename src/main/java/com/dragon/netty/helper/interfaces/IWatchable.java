package com.dragon.netty.helper.interfaces;

/**
 * 监听接口
 * 与IAnswerService 配合使用
 */
public interface IWatchable {
    /**
     * 监听并执行响应动作
     */
    void watchAndNotify();
}
