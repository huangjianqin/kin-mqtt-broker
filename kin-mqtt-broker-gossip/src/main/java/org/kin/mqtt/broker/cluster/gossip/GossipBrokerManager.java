package org.kin.mqtt.broker.cluster.gossip;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.codec.jackson.JacksonMessageCodec;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import io.scalecube.reactor.RetryNonSerializedEmitFailureHandler;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import org.kin.framework.utils.ClassUtils;
import org.kin.framework.utils.JSON;
import org.kin.framework.utils.NetUtils;
import org.kin.mqtt.broker.cluster.BrokerManager;
import org.kin.mqtt.broker.cluster.MqttBrokerNode;
import org.kin.mqtt.broker.cluster.event.MqttClusterEvent;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 基于gossip集群发现机制
 *
 * @author huangjianqin
 * @date 2022/11/16
 */
public final class GossipBrokerManager implements BrokerManager {
    private static final Logger log = LoggerFactory.getLogger(GossipBrokerManager.class);

    /** gossip消息header, 标识mqtt消息 */
    private static final String HEADER_MQTT_MESSAGE = "MqttMessage";
    /** gossip消息header, 标识mqtt事件 */
    private static final String HEADER_MQTT_EVENT = "MqttEvent";
    /** gossip消息header, 标识mqtt事件类型 */
    private static final String HEADER_MQTT_EVENT_TYPE = "MqttEventType";

    /** config */
    private final GossipConfig config;
    /**
     * mqtt broker context
     * lazy init
     */
    private MqttBrokerContext brokerContext;
    /** 来自集群广播的mqtt消息流 */
    private final Sinks.Many<MqttMessageReplica> clusterMqttMessageSink = Sinks.many().multicast().onBackpressureBuffer();
    /** gossip cluster */
    private volatile Mono<Cluster> clusterMono;

    public GossipBrokerManager(GossipConfig config) {
        this.config = config;
    }

    @Override
    public Mono<Void> start(MqttBrokerContext brokerContext) {
        this.brokerContext = brokerContext;
        int port = config.getPort();
        clusterMono = new ClusterImpl().config(clusterConfig -> clusterConfig.externalHost(NetUtils.getIp()).externalPort(port))
                .membership(membershipConfig -> membershipConfig.seedMembers(seedMembers(config.getSeeds()))
                        .namespace(config.getNamespace())
                        .syncInterval(5_000))
                .transport(transportConfig -> transportConfig.transportFactory(new TcpTransportFactory())
                        .messageCodec(JacksonMessageCodec.INSTANCE)
                        .port(port))
                .handler(c -> new GossipMessageHandler())
                .start();
        return clusterMono.then();
    }

    /**
     * gossip member host
     */
    private List<Address> seedMembers(String seeds) {
        //解析seeds
        return Stream.of(seeds.split(";"))
                .map(hostPort -> {
                    String[] splits = hostPort.split(":");
                    return Address.create(splits[0], Integer.parseInt(splits[1]));
                })
                .collect(Collectors.toList());
    }

    @Override
    public Flux<MqttMessageReplica> clusterMqttMessages() {
        return clusterMqttMessageSink.asFlux();
    }

    @Override
    public Flux<MqttBrokerNode> getClusterBrokerNodes() {
        return clusterMono.flatMapIterable(Cluster::members)
                .map(this::clusterNode);
    }

    /**
     * 转换为集群broker信息
     *
     * @param member 集群broker信息
     * @return {@link MqttBrokerNode}
     */
    private MqttBrokerNode clusterNode(Member member) {
        return MqttBrokerNode.builder()
                .name(member.alias())
                .host(member.address().host())
                .port(member.address().port())
                .namespace(member.namespace())
                .build();
    }

    @Override
    public Mono<Void> broadcastMqttMessage(MqttMessageReplica message) {
        // TODO: 2022/12/4 只想那些有订阅topic的节点广播, 提高性能
        return clusterMono.flatMap(c -> {
                    log.debug("cluster broadcast message {} ", message);
                    return c.spreadGossip(Message.builder()
                            .header(HEADER_MQTT_MESSAGE, "true")
                            .data(message)
                            .build());
                })
                .then();
    }

    @Override
    public Mono<Void> broadcastEvent(MqttClusterEvent event) {
        return clusterMono.flatMap(c -> {
                    log.debug("cluster broadcast event {} ", event);
                    return c.spreadGossip(Message.builder()
                            .header(HEADER_MQTT_EVENT, "true")
                            .header(HEADER_MQTT_EVENT_TYPE, event.getClass().getName())
                            .data(JSON.write(event))
                            .build());
                })
                .then();
    }

    @Override
    public Mono<Void> shutdown() {
        return Mono.fromRunnable(() -> {
            clusterMono.subscribe(Cluster::shutdown);
            //close sink
            clusterMqttMessageSink.emitComplete(new RetryNonSerializedEmitFailureHandler());
        });
    }

    //------------------------------------------------------------------------------------------------------------------------

    /**
     * gossip消息处理
     */
    private class GossipMessageHandler implements ClusterMessageHandler {
        @Override
        public void onMessage(Message message) {
            log.warn("cluster is not support to handle message {} ", message);
        }

        @Override
        public void onGossip(Message message) {
            log.debug("cluster accept message {} ", message);
            Map<String, String> headers = message.headers();
            if (headers.containsKey(HEADER_MQTT_MESSAGE)) {
                //mqtt message
                clusterMqttMessageSink.emitNext(message.data(), new RetryNonSerializedEmitFailureHandler());
            } else if (headers.containsKey(HEADER_MQTT_EVENT)) {
                //mqtt event
                String eventTypeStr = null;
                Class<? extends MqttClusterEvent> eventType;
                try {
                    eventTypeStr = headers.getOrDefault(HEADER_MQTT_EVENT_TYPE, "");
                    eventType = ClassUtils.getClass(eventTypeStr);
                } catch (Exception e) {
                    throw new IllegalStateException(String.format("unknown mqtt event class '%s'", eventTypeStr));
                }

                brokerContext.broadcastEvent(JSON.read((String) message.data(), eventType));
            }
        }

        @Override
        public void onMembershipEvent(MembershipEvent event) {
            Member member = event.member();
            StringJoiner sj = new StringJoiner(":");
            sj.add(member.namespace());
            sj.add(member.alias());
            String address = member.address().toString();
            sj.add(address);
            log.info("mqtt broker(namespace:alias:address) '{}' {}", sj, event.type());
            // TODO: 2022/11/28 是否存储集群所有broker的broker id
            switch (event.type()) {
                case ADDED:
                    break;
                case LEAVING:
                    break;
                case REMOVED:
                    break;
                case UPDATED:
                    break;
                default:
                    break;
            }
        }
    }
}
