package org.kin.mqtt.broker.cluster;

import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * mqtt集群broker管理
 * 目前支持
 * 1. 单节点
 * 2. 基于gossip集群节点发现
 * 3. (集群实现方案, 供参考, 暂不支持)基于zk, nacos, spring cloud等注册中心的节点发现机制+rpc广播消息
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public interface BrokerManager {
    /**
     * start
     *
     * @return init complete signal
     */
    Mono<Void> start();

    /**
     * 返回来自集群其他broker的mqtt消息流
     *
     * @return 来自集群其他broker的mqtt消息流
     */
    Flux<MqttMessageReplica> clusterMqttMessages();

    /**
     * 返回集群所有broker节点信息
     *
     * @return 集群所有broker节点信息
     */
    Flux<MqttBrokerNode> getClusterBrokerNodes();

    /**
     * 集群广播mqtt消息, 目前广播publish消息
     *
     * @param message 要广播的mqtt消息
     * @return broadcast complete signal
     */
    Mono<Void> broadcastMqttMessage(MqttMessageReplica message);

    /**
     * shutdown
     *
     * @return shutdown complete signal
     */
    Mono<Void> shutdown();
}
