package org.kin.mqtt.broker.core;

import io.netty.util.HashedWheelTimer;
import org.kin.framework.Closeable;
import org.kin.framework.reactor.event.DefaultReactorEventBus;
import org.kin.framework.reactor.event.ReactorEventBus;
import org.kin.framework.utils.SysUtils;
import org.kin.mqtt.broker.acl.AclService;
import org.kin.mqtt.broker.auth.AuthService;
import org.kin.mqtt.broker.bridge.BridgeManager;
import org.kin.mqtt.broker.core.cluster.BrokerManager;
import org.kin.mqtt.broker.core.cluster.Cluster;
import org.kin.mqtt.broker.core.cluster.ClusterStore;
import org.kin.mqtt.broker.core.cluster.MqttSessionStore;
import org.kin.mqtt.broker.core.cluster.event.MqttClusterEvent;
import org.kin.mqtt.broker.core.event.MqttEvent;
import org.kin.mqtt.broker.core.event.MqttEventConsumer;
import org.kin.mqtt.broker.core.retry.RetryService;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.share.ShareSubLoadBalance;
import org.kin.mqtt.broker.rule.RuleEngine;
import org.kin.mqtt.broker.rule.RuleManager;
import org.kin.mqtt.broker.store.MqttMessageStore;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * 包含一些mqtt broker共享资源, 全局唯一
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class MqttBrokerContext implements Closeable {
    private final MqttBrokerConfig brokerConfig;
    /** mqtt消息处理的{@link Scheduler} todo 如果datastore datasource auth能支持全异步的形式, 则不需要额外的scheduler也ok */
    private final Scheduler mqttBizScheduler;
    /** retry task管理 */
    private final RetryService retryService = new DefaultRetryService();
    /** topic管理 */
    private final TopicManager topicManager;
    /** session管理 */
    private final MqttSessionManager sessionManager = new DefaultMqttSessionManager();
    /** mqtt消息处理实现 */
    private final MqttMessageDispatcher dispatcher;
    /** auth service */
    private final AuthService authService;
    /** mqtt broker cluster */
    private final Cluster cluster;
    /** mqtt消息外部存储 */
    private final MqttMessageStore messageStore;
    /** 规则链管理 */
    private final RuleManager ruleManager = new RuleManager(this);
    /** 规则链执行 */
    private final RuleEngine ruleEngine = new RuleEngine(ruleManager);
    /** 数据桥接实现管理 */
    private final BridgeManager bridgeManager;
    /** 访问控制权限管理 */
    private final AclService aclService;
    /** 事件总线 */
    private final ReactorEventBus eventBus;
    /** 业务相关定时器 */
    private final HashedWheelTimer bsTimer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS, 10);

    @SuppressWarnings("rawtypes")
    public MqttBrokerContext(MqttBrokerConfig brokerConfig, MqttMessageDispatcher dispatcher,
                             AuthService authService, MqttMessageStore messageStore, AclService aclService,
                             ShareSubLoadBalance shareSubLoadBalance, Collection<MqttEventConsumer> eventConsumers) {
        this.brokerConfig = brokerConfig;
        this.mqttBizScheduler = Schedulers.newBoundedElastic(SysUtils.CPU_NUM * 10, Integer.MAX_VALUE, "kin-mqtt-broker-bs-" + brokerConfig.getPort(), 60);
        this.topicManager = new DefaultTopicManager(shareSubLoadBalance);
        this.dispatcher = dispatcher;
        this.authService = authService;
        this.cluster = new Cluster(this);
        this.messageStore = messageStore;
        this.bridgeManager = new BridgeManager(this);
        this.aclService = aclService;
        this.eventBus = new DefaultReactorEventBus(true, mqttBizScheduler);
        for (Object eventConsumer : eventConsumers) {
            this.eventBus.register(eventConsumer);
        }
    }

    @Override
    public void close() {
        //retry close
        retryService.close();
        //bridge close
        bridgeManager.close();
        //cluster close
        cluster.shutdown().subscribe();

        mqttBizScheduler.dispose();
    }

    /**
     * 广播事件
     */
    public void broadcastEvent(MqttEvent event) {
        eventBus.post(event);
    }

    /**
     * 广播集群事件
     */
    public void broadcastClusterEvent(MqttClusterEvent event) {
        cluster.getBrokerManager().broadcastEvent(event).subscribe();
    }

    /**
     * @return mqtt broker主动往client发送mqtt消息, 消息头所设置的clientId
     */
    public String getBrokerClientId() {
        return "MQTTBroker-" + brokerConfig.getBrokerId();
    }

    //getter
    public MqttBrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public String getBrokerId() {
        return getBrokerConfig().getBrokerId();
    }

    public Scheduler getMqttBizScheduler() {
        return mqttBizScheduler;
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

    public MqttSessionStore getSessionStore() {
        return cluster.getSessionStore();
    }

    public MqttSessionManager getSessionManager() {
        return sessionManager;
    }

    public AuthService getAuthService() {
        return authService;
    }

    public MqttMessageDispatcher getDispatcher() {
        return dispatcher;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public BrokerManager getBrokerManager() {
        return getCluster().getBrokerManager();
    }

    public ClusterStore getClusterStore() {
        return getCluster().getClusterStore();
    }

    public RuleManager getRuleManager() {
        return ruleManager;
    }

    public RuleEngine getRuleEngine() {
        return ruleEngine;
    }

    public AclService getAclService() {
        return aclService;
    }

    public ReactorEventBus getEventBus() {
        return eventBus;
    }

    public BridgeManager getBridgeManager() {
        return bridgeManager;
    }

    public HashedWheelTimer getBsTimer() {
        return bsTimer;
    }
}