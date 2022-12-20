package org.kin.mqtt.broker.cluster;

import org.kin.mqtt.broker.cluster.event.MqttClusterEvent;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 单节点启动
 *
 * @author huangjianqin
 * @date 2022/11/16
 */
public class StandaloneBrokerManager implements BrokerManager {
    /** 单例 */
    public static final BrokerManager INSTANCE = new StandaloneBrokerManager();

    private StandaloneBrokerManager() {
    }

    @Override
    public Mono<Void> start(MqttBrokerContext brokerContext) {
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
    public Mono<Void> broadcastEvent(MqttClusterEvent event) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> shutdown() {
        return Mono.empty();
    }
}
