package org.kin.mqtt.broker.core.retry;

import io.netty.util.TimerTask;

import java.util.concurrent.TimeUnit;

/**
 * 针对已发送的qos=012的消息重试task
 *
 * @author huangjianqin
 * @date 2022/11/13
 */
public interface Retry extends TimerTask {
    /**
     * 停止重试
     */
    void cancel();

    /**
     * @return retry id
     */
    long getId();

    /**
     * @return retry间隔
     */
    int getDelay();

    /**
     * @return retry间隔时间单位
     */
    TimeUnit getUnit();
}
