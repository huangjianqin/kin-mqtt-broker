package org.kin.mqtt.broker.core.cluster;

import org.kin.mqtt.broker.core.cluster.event.MqttClusterEvent;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;

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
    public Mono<Void> init() {
        return Mono.empty();
    }

    @Override
    public Flux<MqttMessageReplica> clusterMqttMessages() {
        return Flux.empty();
    }

    @Override
    public Mono<Void> broadcastMqttMessage(MqttMessageReplica message) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> sendMqttMessage(String remoteBrokerId, String clientId, MqttMessageReplica message) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> broadcastEvent(MqttClusterEvent event) {
        return Mono.empty();
    }

    @Override
    public MqttBrokerNode getNodeById(String brokerId) {
        return null;
    }

    @Override
    public Collection<MqttBrokerNode> getClusterBrokerNodes() {
        return Collections.emptySet();
    }

    @Override
    public Mono<Void> shutdown() {
        return Mono.empty();
    }
}
