package org.kin.mqtt.broker.core.retry;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.ReactorNetty;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 针对qos=012的消息重试publish
 *
 * @author huangjianqin
 * @date 2022/11/13
 */
public class PublishRetry implements Retry {
    private static final Logger log = LoggerFactory.getLogger(PublishRetry.class);
    /** 默认最大重试次数 */
    public static final int DEFAULT_MAX_RETRY_TIMES = 5;
    /** 默认重试间隔 */
    public static final int DEFAULT_PERIOD = 3;

    private final long id;
    /** 最大重试次数 */
    private final int maxTimes;
    /** 重试间隔 */
    private final int period;
    /** 重试发送的mqtt message */
    private final MqttMessage message;
    /** 重试逻辑 */
    private final Consumer<MqttMessage> retryTask;
    /** 所属{@link RetryService} */
    private final RetryService retryService;
    /** 当前已重试次数 */
    private int curTimes;
    /** 是否已被取消 */
    private volatile boolean cancelled = false;
    /** 是否已执行{@link #onClean()} */
    private volatile AtomicBoolean cleaned = new AtomicBoolean();

    public PublishRetry(long id, MqttMessage message, Consumer<MqttMessage> retryTask, RetryService retryService) {
        this(id, DEFAULT_MAX_RETRY_TIMES, DEFAULT_PERIOD, message, retryTask, retryService);
    }

    public PublishRetry(long id, int maxTimes, int period, MqttMessage message, Consumer<MqttMessage> retryTask, RetryService retryService) {
        this.id = id;
        this.maxTimes = maxTimes;
        this.period = period;
        this.message = message;
        this.retryTask = retryTask;
        this.retryService = retryService;
    }

    /**
     * 获取要发送的mqtt消息
     *
     * @param mqttMessage mqtt消息
     * @return 要发送的mqtt消息
     */
    public static MqttMessage getReplyMqttMessage(MqttMessage mqttMessage) {
        if (mqttMessage instanceof MqttPublishMessage) {
            return ((MqttPublishMessage) mqttMessage).copy();
        } else {
            return mqttMessage;
        }
    }

    @Override
    public void run(Timeout timeout){
        if (!cancelled && curTimes++ < maxTimes) {
            try {
                log.debug("id={} retry publish", getId());
                MqttMessage message = getReplyMqttMessage(this.message);
                retryTask.accept(message);
                retryService.execRetry(this);
            } catch (Exception e) {
                log.error("", e);
            }
        } else {
            //下一次重试发现已取消, 则clean, 即异步clean
            onClean();
        }
    }

    @Override
    public void cancel() {
        log.debug("id={} retry cancelled", getId());
        cancelled = true;
        onClean();
    }

    /**
     * 执行retry结束后的clean逻辑
     */
    private void onClean() {
        if (!cleaned.compareAndSet(false, true)) {
            return;
        }

        retryService.removeRetry(getId());
        ReactorNetty.safeRelease(this.message);
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public int getDelay() {
        return period * (curTimes + 1);
    }

    @Override
    public TimeUnit getUnit() {
        return TimeUnit.SECONDS;
    }

    @Override
    public String toString() {
        return "PublishRetry{" +
                "id=" + id +
                ", maxTimes=" + maxTimes +
                ", period=" + period +
                ", curTimes=" + curTimes +
                '}';
    }
}
