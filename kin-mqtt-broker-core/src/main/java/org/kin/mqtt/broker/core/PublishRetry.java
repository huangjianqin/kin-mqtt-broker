package org.kin.mqtt.broker.core;

import io.netty.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 针对qos=012的消息重试publish
 *
 * @author huangjianqin
 * @date 2022/11/13
 */
public final class PublishRetry implements Retry {
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
    /** 重试逻辑 */
    private final Runnable retryTask;
    /** 结束重试后操作, 异步 */
    private final Runnable cleaner;
    /** 所属{@link RetryService} */
    private final RetryService retryService;
    /** 当前已重试次数 */
    private int curTimes;
    /** 是否已被取消 */
    private volatile boolean cancelled = false;

    public PublishRetry(long id, Runnable retryTask, Runnable cleaner, RetryService retryService) {
        this(id, DEFAULT_MAX_RETRY_TIMES, DEFAULT_PERIOD, retryTask, cleaner, retryService);
    }

    public PublishRetry(long id, int maxTimes, int period, Runnable retryTask, Runnable cleaner, RetryService retryService) {
        this.id = id;
        this.maxTimes = maxTimes;
        this.period = period;
        this.retryTask = retryTask;
        this.cleaner = cleaner;
        this.retryService = retryService;
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if (!cancelled && curTimes++ < maxTimes) {
            try {
                log.debug("id={} retry publish", getId());
                retryTask.run();
                retryService.execRetry(this);
            } catch (Exception e) {
                log.error("", e);
            }
        } else {
            //下一次重试发现已取消, 则clean, 即异步clean
            cleaner.run();
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
        retryService.removeRetry(getId());
        log.debug("id={} retry cancelled", getId());
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
