package org.kin.mqtt.broker.core.cluster.standalone;

import com.google.common.base.Preconditions;
import org.kin.mqtt.broker.core.MqttBrokerBootstrap;
import org.kin.mqtt.broker.core.cluster.BrokerManager;
import org.kin.mqtt.broker.core.cluster.MqttBrokerNode;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 单节点启动
 *
 * @author huangjianqin
 * @date 2022/11/16
 */
public final class StandaloneBrokerManager implements BrokerManager {
    /** 单例 */
    public static final BrokerManager INSTANCE = new StandaloneBrokerManager();

    private StandaloneBrokerManager() {
    }

    @Override
    public Mono<Void> init(MqttBrokerBootstrap bootstrap) {
        Preconditions.checkArgument(Objects.isNull(bootstrap.getClusterConfig()));
        return Mono.empty();
    }

    @Override
    public Flux<MqttMessageReplica> clusterMqttMessages() {
        return Flux.empty();
    }

    @Override
    public Flux<MqttBrokerNode> getClusterBrokerNodes() {
        return Flux.empty();
    }

    @Override
    public Mono<Void> broadcastMqttMessage(MqttMessageReplica message) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> shutdown() {
        return Mono.empty();
    }
}
