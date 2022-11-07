package org.kin.mqtt.broker;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * mqtt session
 *
 * @author huangjianqin
 * @date 2022/11/6
 */
public final class MqttSession {
    /** session uuid */
    private final String uuid = UUID.randomUUID().toString();
    /** mqtt连接的client id */
    private final String clientId;
    /** broker返回的publish package id生成器 */
    private final AtomicInteger packageIdGenerator = new AtomicInteger(1);
    /** mqtt订阅管理 */
    private final SubscriptionManager subscriptionManager;
    /** mqtt session 是否已关闭 */
    private volatile boolean closed;

    public MqttSession(String clientId) {
        this.clientId = clientId;
        this.subscriptionManager = new SubscriptionManager(this);
    }

    /**
     * @return 下一publish package id
     */
    public int nextPackageId() {
        if (isClosed()) {
            throw new IllegalStateException("mqtt session has been closed");
        }
        return packageIdGenerator.getAndIncrement();
    }

    /**
     * session关闭
     */
    public void close() {
        closed = true;
        subscriptionManager.unsubscribeAll();
    }

    //getter
    public String getUuid() {
        return uuid;
    }

    public String getClientId() {
        return clientId;
    }

    public SubscriptionManager getSubscriptionManager() {
        return subscriptionManager;
    }

    public boolean isClosed() {
        return closed;
    }
}
