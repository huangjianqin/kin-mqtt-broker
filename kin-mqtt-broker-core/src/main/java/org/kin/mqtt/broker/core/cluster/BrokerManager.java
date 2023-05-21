package org.kin.mqtt.broker.core.cluster;

import org.kin.framework.log.LoggerOprs;
import org.kin.mqtt.broker.core.cluster.event.MqttClusterEvent;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * mqtt集群broker节点管理
 * 目前支持
 * 1. 单节点
 * 2. 基于gossip去中心化集群节点发现
 *
 * @author huangjianqin
 * @date 2022/11/15
 */
public interface BrokerManager extends LoggerOprs {
    /**
     * start
     *
     * @return init complete signal
     */
    Mono<Void> init();

    /**
     * 返回来自集群其他broker的mqtt消息流
     *
     * @return 来自集群其他broker的mqtt消息流
     */
    Flux<MqttMessageReplica> clusterMqttMessages();

    /**
     * 集群广播mqtt消息, 目前仅广播publish消息
     * <p>
     * 如果一个client同时连两个broker, 那么会受到两条pub消息, 当然, 我们也不允许这种情况发生
     * 一般来说, 一client同一时间内只会连接一broker, 但broker要确保session有效才publish订阅消息,
     * eclipse mqtt client可以设置多个mqtt server地址,
     * 在connect时, 其中一broker连接失败, 则通过index+1才寻找下一broker, 直到连接成功;
     * 如果连接断开, 则发起重连, 重连会从index=0的broker开始尝试connect.
     *
     * @param message 要广播的mqtt消息
     * @return broadcast complete signal
     */
    Mono<Void> broadcastMqttMessage(MqttMessageReplica message);

    /**
     * 给与remote broker连接的mqtt client发送publish消息
     *
     * @param remoteBrokerId remote broker id
     * @param clientId       目标mqtt client id
     * @param message        mqtt publish消息
     * @return
     */
    Mono<Void> sendMqttMessage(String remoteBrokerId, String clientId, MqttMessageReplica message);

    /**
     * 集群广播集群事件
     *
     * @param event 要广播的集群事件
     * @return broadcast complete signal
     */
    Mono<Void> broadcastEvent(MqttClusterEvent event);

    /**
     * 根据broker id获取{@link MqttBrokerNode}实例
     *
     * @param brokerId mqtt broker id
     * @return {@link MqttBrokerNode}实例
     */
    @Nullable
    MqttBrokerNode getNodeById(String brokerId);

    /**
     * 返回集群所有broker节点信息
     *
     * @return 集群所有broker节点信息
     */
    Collection<MqttBrokerNode> getClusterBrokerNodes();

    /**
     * 返回集群所有broker节点信息{@link Flux}实例
     *
     * @return 集群所有broker节点信息{@link Flux}实例
     */
    default Flux<MqttBrokerNode> getClusterBrokerNodesFlux(){
        return Flux.fromIterable(getClusterBrokerNodes());
    }

    /**
     * shutdown
     *
     * @return shutdown complete signal
     */
    Mono<Void> shutdown();
}
