package org.kin.mqtt.broker.core.cluster;

import org.kin.framework.reactor.event.ReactorEventBus;
import org.kin.mqtt.broker.core.MqttBrokerConfig;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.cluster.event.BrokerSubscriptionsChangedEvent;
import org.kin.mqtt.broker.core.event.MqttEventConsumer;
import org.kin.mqtt.broker.core.event.MqttSubscribeEvent;
import org.kin.mqtt.broker.core.event.MqttUnsubscribeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * mqtt broker集群
 *
 * @author huangjianqin
 * @date 2023/5/18
 */
public final class Cluster {
    private static final Logger log = LoggerFactory.getLogger(Cluster.class);

    /** mqtt broker context */
    private final MqttBrokerContext brokerContext;
    /** 集群数据访问接口 */
    private final ClusterStore clusterStore;
    /** 集群broker节点管理 */
    private final BrokerManager brokerManager;
    /** 是否为集群模式 */
    private final boolean cluster;
    /** 是否为core节点 */
    private final boolean core;
    /** 基于cluster store的{@link MqttSessionStore}实现 */
    private final MqttSessionStore sessionStore;

    public Cluster(MqttBrokerContext brokerContext) {
        this.brokerContext = brokerContext;
        ClusterConfig config = brokerContext.getBrokerConfig().getCluster();
        this.cluster = config.isClusterMode();
        this.core = !this.cluster || config.isCore();

        this.clusterStore = this.cluster ? new RaftKvStore(brokerContext, this) :
                new StandaloneKvStore(this);
        this.brokerManager = this.cluster ? new GossipBrokerManager(this,
                node -> {
                    String storeAddress = node.getStoreAddress();
                    if (node.isCore()) {
                        clusterStore.addCore(storeAddress);
                    }
                    else{
                        clusterStore.addReplicator(storeAddress);
                    }
                },
                node -> {
                    String storeAddress = node.getStoreAddress();
                    if (node.isCore()) {
                        clusterStore.removeCore(storeAddress);
                    }
                    else{
                        clusterStore.removeReplicator(storeAddress);
                    }
                }) :
                StandaloneBrokerManager.INSTANCE;
        this.sessionStore = new DefaultMqttSessionStore(this.clusterStore);
    }

    /**
     * 初始化集群环境
     */
    public Mono<Void> init() {
        return Mono.when(clusterStore.init(), brokerManager.init())
                .then(Mono.fromRunnable(() -> {
                    //注册内部consumer
                    ReactorEventBus eventBus = brokerContext.getEventBus();
                    eventBus.register(new MqttSubscribeEventConsumer());
                    eventBus.register(new MqttUnsubscribeEventConsumer());
                }));
    }

    /**
     * 更新本broker的订阅信息, 然后广播集群其他broker, 更新本broker的订阅信息
     */
    private void onSubscriptionChanged() {
        //广播集群其他broker, 更新本broker的订阅信息
        String brokerId = brokerContext.getBrokerId();
        String key = ClusterStoreKeys.getBrokerSubscriptionKey(brokerId);
        clusterStore.put(key, new BrokerSubscriptions(brokerContext.getTopicManager().getAllSubRegexTopics()))
                .then(brokerManager.broadcastEvent(new BrokerSubscriptionsChangedEvent(brokerId)))
                .subscribe();
    }

    /**
     * shutdown
     *
     * @return shutdown complete signal
     */
    public Mono<Void> shutdown() {
        return brokerManager.shutdown().
                then(Mono.fromRunnable(clusterStore::shutdown));
    }

    //getter
    public boolean isCore() {
        return core;
    }

    public boolean isCluster() {
        return cluster;
    }

    public boolean isStandalone() {
        return !isCluster();
    }

    public ClusterConfig getConfig() {
        return getBrokerConfig().getCluster();
    }

    public MqttBrokerConfig getBrokerConfig() {
        return brokerContext.getBrokerConfig();
    }

    public BrokerManager getBrokerManager() {
        return brokerManager;
    }

    public MqttBrokerContext getBrokerContext() {
        return brokerContext;
    }

    public ClusterStore getClusterStore() {
        return clusterStore;
    }

    public MqttSessionStore getSessionStore() {
        return sessionStore;
    }

    //--------------------------------------------------------internal mqtt event consumer

    /**
     * 注册订阅consumer
     */
    private class MqttSubscribeEventConsumer implements MqttEventConsumer<MqttSubscribeEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, MqttSubscribeEvent event) {
            onSubscriptionChanged();
        }
    }

    /**
     * 取消订阅consumer
     */
    private class MqttUnsubscribeEventConsumer implements MqttEventConsumer<MqttUnsubscribeEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, MqttUnsubscribeEvent event) {
            onSubscriptionChanged();
        }
    }
}
