package org.kin.mqtt.broker.core.cluster;

import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
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

    /** 集群配置 */
    private final ClusterConfig config;
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

    public Cluster(MqttBrokerContext brokerContext, ClusterConfig config) {
        this.config = config;
        this.brokerContext = brokerContext;
        this.cluster = config.isClusterMode();
        this.core = !this.cluster || config.isCore();

        this.clusterStore = this.cluster ? new RaftKvStore(brokerContext, this) :
                new StandaloneKvStore(this);
        this.brokerManager = this.cluster ? new GossipBrokerManager(this,
                metadata -> clusterStore.addReplicator(metadata.getStoreAddress()),
                metadata -> clusterStore.removeReplicator(metadata.getStoreAddress())) :
                StandaloneBrokerManager.INSTANCE;
    }

    /**
     * 初始化集群环境
     */
    public void init() {
        clusterStore.init();

        brokerManager.init()
                .then(Mono.fromRunnable(() -> brokerManager.clusterMqttMessages()
                        .onErrorResume(e -> Mono.empty())
                        .publishOn(brokerContext.getMqttBizScheduler())
                        .flatMap(mqttMessageReplica -> brokerContext.getDispatcher()
                                .dispatch(MqttMessageContext.fromCluster(mqttMessageReplica), brokerContext))
                        .subscribe((mqttMessageReplica) -> {
                                },
                                t -> brokerManager.error("broker manager handle cluster message error", t))))
                .subscribe();
    }

    /**
     * shutdown
     *
     * @return shutdown complete signal
     */
    public Mono<Void> shutdown() {
        return brokerManager.shutdown().
                doOnNext(v -> clusterStore.shutdown());
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
        return config;
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
}
