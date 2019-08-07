package com.captain.utils.mq.rabbitmq;

/**
 * 消息队列
 * 内容处理类
 *
 * @author hryuan911
 * @date 2019/8/7
 * @param
 * @return
 */
@FunctionalInterface
public interface ProcessingInterface<V> {
    V processing(String message) throws Exception;
}
