package org.kin.mqtt.broker.core;

import org.kin.framework.Closeable;
import org.kin.framework.utils.SysUtils;
import org.kin.mqtt.broker.auth.AuthService;
import org.kin.mqtt.broker.cluster.BrokerManager;
import org.kin.mqtt.broker.core.store.MqttMessageStore;
import org.kin.mqtt.broker.core.topic.TopicManager;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * 包含一些mqtt broker共享资源, 全局唯一
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public final class MqttBrokerContext implements Closeable {
    /** mqtt消息处理的{@link Scheduler} todo 如果datastore datasource auth能支持全异步的形式, 则不需要额外的scheduler也ok */
    public final Scheduler mqttMessageHandleScheduler;
    /** retry task管理 */
    private final RetryService retryService = new DefaultRetryService();
    /** topic管理 */
    private final TopicManager topicManager = new DefaultTopicManager();
    /** channel管理 */
    private final MqttChannelManager channelManager = new DefaultMqttChannelManager();
    /** mqtt消息处理实现 */
    private final MqttMessageDispatcher dispatcher;
    /** auth service */
    private final AuthService authService;
    /** mqtt broker集群管理 */
    private final BrokerManager brokerManager;
    /** mqtt消息外部存储 */
    private final MqttMessageStore messageStore;

    public MqttBrokerContext(int port, MqttMessageDispatcher dispatcher, AuthService authService, BrokerManager brokerManager, MqttMessageStore messageStore) {
        mqttMessageHandleScheduler = Schedulers.newBoundedElastic(SysUtils.CPU_NUM * 10, Integer.MAX_VALUE, "kin-mqtt-broker-bs-" + port, 60);
        this.dispatcher = dispatcher;
        this.authService = authService;
        this.brokerManager = brokerManager;
        this.messageStore = messageStore;
    }

    @Override
    public void close() {
        retryService.close();
        brokerManager.shutdown().subscribe();
        mqttMessageHandleScheduler.dispose();
    }

    //getter
    public Scheduler getMqttMessageHandleScheduler() {
        return mqttMessageHandleScheduler;
    }

    public RetryService getRetryService() {
        return retryService;
    }

    public TopicManager getTopicManager() {
        return topicManager;
    }

    public MqttMessageStore getMessageStore() {
        return messageStore;
    }

    public MqttChannelManager getChannelManager() {
        return channelManager;
    }

    public AuthService getAuthService() {
        return authService;
    }

    public MqttMessageDispatcher getDispatcher() {
        return dispatcher;
    }

    public BrokerManager getBrokerManager() {
        return brokerManager;
    }
}
