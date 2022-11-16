package org.kin.mqtt.broker.core;

import org.kin.framework.Closeable;

/**
 * 针对已发送的qos=012的消息进行重试管理
 *
 * @author huangjianqin
 * @date 2022/11/13
 */
public interface RetryService extends Closeable {
    /**
     * 执行retry task
     *
     * @param retry retry task
     */
    void execRetry(Retry retry);

    /**
     * 根据id获取retry task
     *
     * @param id retry id
     * @return retry task
     */
    Retry getRetry(long id);

    /**
     * 移除retry
     *
     * @param id retry id
     */
    void removeRetry(long id);
}
