package org.kin.mqtt.broker.core.cluster;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.codec.jackson.JacksonMessageCodec;
import io.scalecube.cluster.codec.jackson.JacksonMetadataCodec;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import io.scalecube.reactor.RetryNonSerializedEmitFailureHandler;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.utils.ClassUtils;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.cluster.event.AbstractMqttClusterEvent;
import org.kin.mqtt.broker.core.cluster.event.MqttClusterEvent;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 基于gossip集群发现mqtt broker节点
 * <p>
 * 集群broker间订阅同步机制:
 * 最终一致性, 不能保证集群各broker节点及时看到订阅关系变化
 * 基于临时(非持久化)事务id(tid)做增量合并, tid是基于broker+topic生成的, tid在broker重启后会重置,
 * 最极端的情况是哪怕某个broker节点没有任何订阅, 其余节点都存储着该broker节点之前的任何订阅信息(但会被标识为无效订阅)
 *
 * @author huangjianqin
 * @date 2022/11/16
 */
public class GossipBrokerManager implements BrokerManager {
    /** gossip消息header, 标识mqtt事件类型 */
    private static final String HEADER_MQTT_EVENT_TYPE = "MqttEventType";
    /** gossip消息header, 标识目标mqtt clientId */
    private static final String HEADER_MQTT_CLIENT_ID = "MqttClientId";
    /** 默认gossip集群namespace */
    private static final String DEFAULT_NAMESPACE = "KinMqttBroker";

    /** 集群配置 */
    private final org.kin.mqtt.broker.core.cluster.Cluster mqttBrokerCluster;
    /**
     * mqtt broker context
     * lazy init
     */
    private final MqttBrokerContext brokerContext;
    /** gossip cluster */
    private Mono<Cluster> clusterMono;
    /** 来自集群广播的mqtt消息流 */
    private final Sinks.Many<MqttMessageReplica> clusterMqttMessageSink = Sinks.many().unicast().onBackpressureBuffer();
    /** remote集群节点信息, key -> host:port */
    private final Map<String, MqttBrokerNode> clusterBrokers = new NonBlockingHashMap<>();
    /** 新broker加入集群处理逻辑 */
    @Nullable
    private final Consumer<MqttBrokerMetadata> brokerAddPostProcessor;
    /** broker从集群移除处理逻辑 */
    @Nullable
    private final Consumer<MqttBrokerMetadata> brokerRemovePostProcessor;

    public GossipBrokerManager(org.kin.mqtt.broker.core.cluster.Cluster mqttBrokerCluster) {
        this(mqttBrokerCluster, null, null);
    }

    public GossipBrokerManager(org.kin.mqtt.broker.core.cluster.Cluster mqttBrokerCluster,
                               @Nullable Consumer<MqttBrokerMetadata> brokerAddPostProcessor,
                               @Nullable Consumer<MqttBrokerMetadata> brokerRemovePostProcessor) {
        this.mqttBrokerCluster = mqttBrokerCluster;
        this.brokerContext = mqttBrokerCluster.getBrokerContext();
        this.brokerAddPostProcessor = brokerAddPostProcessor;
        this.brokerRemovePostProcessor = brokerRemovePostProcessor;
    }

    @Override
    public Mono<Void> init() {
        ClusterConfig config = mqttBrokerCluster.getConfig();

        int port = config.getPort();
        clusterMono = new ClusterImpl().config(clusterConfig -> clusterConfig.externalHost(config.getHost())
                        //gossip节点名即broker id
                        .memberAlias(brokerContext.getBrokerId())
                        .externalPort(port)
                        .metadata(MqttBrokerMetadata.create(mqttBrokerCluster)))
                .membership(membershipConfig -> membershipConfig.seedMembers(seedMembers(config.getSeeds()))
                        .namespace(DEFAULT_NAMESPACE)
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
        return Stream.of(seeds.split(",")).map(hostPort -> {
            String[] splits = hostPort.split(":");
            return Address.create(splits[0], Integer.parseInt(splits[1]));
        }).collect(Collectors.toList());
    }

    @Override
    public Flux<MqttMessageReplica> clusterMqttMessages() {
        return clusterMqttMessageSink.asFlux();
    }

    @Override
    public Mono<Void> broadcastMqttMessage(MqttMessageReplica message) {
        return Flux.fromIterable(clusterBrokers.values())
                //过滤没有订阅的broker节点
                .filter(n -> n.hasSubscription(message.getTopic()))
                //只往有订阅的broker节点广播publish消息
                .flatMap(n -> clusterMono.flatMap(c -> {
                    debug("broadcast cluster message {} to node({}:{}:{})", message, n.getId(), n.getHost(), n.getPort());
                    return c.send(Address.create(n.getHost(), n.getPort()), Message.builder().data(message).build());
                })).then();
    }

    @Override
    public Mono<Void> sendMqttMessage(String remoteBrokerId, String clientId, MqttMessageReplica message) {
        return Flux.fromIterable(clusterBrokers.values()).filter(n -> n.getId().equals(remoteBrokerId)).flatMap(n -> clusterMono.flatMap(c -> {
            debug("send cluster message {} to node({}:{}:{})", message, n.getId(), n.getHost(), n.getPort());
            return c.send(Address.create(n.getHost(), n.getPort()), Message.builder().header(HEADER_MQTT_CLIENT_ID, clientId).data(message).build());
        })).then();
    }

    @Override
    public Mono<Void> broadcastEvent(MqttClusterEvent event) {
        if (event instanceof AbstractMqttClusterEvent) {
            ((AbstractMqttClusterEvent) event).setAddress(mqttBrokerCluster.getConfig().getAddress());
        }
        return clusterMono.flatMap(c -> {
            debug("broadcast cluster event {} ", event);
            return c.spreadGossip(Message.builder().header(HEADER_MQTT_EVENT_TYPE, event.getClass().getName()).data(JSON.write(event)).build());
        }).then();
    }

    @Override
    public MqttBrokerNode getNode(String address) {
        return clusterBrokers.get(address);
    }

    @Override
    public Collection<MqttBrokerNode> getClusterBrokerNodes() {
        return clusterBrokers.values();
    }

    @Override
    public Mono<Void> shutdown() {
        return clusterMono.flatMap(cluster -> cluster.onShutdown()
                //close sink
                .then(Mono.fromRunnable(() -> clusterMqttMessageSink.emitComplete(RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED))));
    }

    //------------------------------------------------------------------------------------------------------------------------

    /**
     * gossip消息处理
     */
    private class GossipMessageHandler implements ClusterMessageHandler {
        @Override
        public void onMessage(Message message) {
            //mqtt publish消息
            debug("accept cluster message {} ", message);
            Map<String, String> headers = message.headers();
            String clientId = headers.get(HEADER_MQTT_CLIENT_ID);
            MqttMessageReplica messageReplica = message.data();
            if (Objects.isNull(clientId)) {
                //mqtt client pub/sub
                clusterMqttMessageSink.emitNext(messageReplica, RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
            } else {
                //broker -> client
                MqttSession mqttSession = brokerContext.getSessionManager().get(clientId);
                if (Objects.isNull(mqttSession)) {
                    return;
                }

                //开启retry
                //不纳入Inflight
                mqttSession.sendMessage(MqttMessageHelper.createPublish(messageReplica), messageReplica.getQos() > 0).subscribe();
            }
        }

        @Override
        public void onGossip(Message message) {
            //mqtt event
            debug("accept gossip cluster message {} ", message);
            Map<String, String> headers = message.headers();
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

        @Override
        public void onMembershipEvent(MembershipEvent event) {
            Member member = event.member();
            String namespace = member.namespace();
            String id = member.alias();
            Address memberAddress = member.address();
            String address = memberAddress.toString();
            info("mqtt broker(namespace:id:address) '{}:{}:{}' {}", namespace, id, address, event.type());
            switch (event.type()) {
                case ADDED:
                    try {
                        MqttBrokerMetadata metadata = (MqttBrokerMetadata) JacksonMetadataCodec.INSTANCE.deserialize(event.newMetadata());

                        MqttBrokerNode brokerNode = MqttBrokerNode.create(id, metadata);
                        clusterBrokers.put(address, brokerNode);

                        if (Objects.nonNull(brokerAddPostProcessor)) {
                            brokerAddPostProcessor.accept(metadata);
                        }
                    } catch (Exception e) {
                        error("broker({}:{}:{}) add post processor error", namespace, id, address, e);
                    }
                    break;
                case LEAVING:
                case REMOVED:
                    try {
                        clusterBrokers.remove(address);

                        MqttBrokerMetadata metadata = (MqttBrokerMetadata) JacksonMetadataCodec.INSTANCE.deserialize(event.oldMetadata());
                        if (Objects.nonNull(brokerRemovePostProcessor)) {
                            brokerRemovePostProcessor.accept(metadata);
                        }
                    } catch (Exception e) {
                        error("broker({}:{}:{}) add post processor error", namespace, id, address, e);
                    }
                    break;
                case UPDATED:
                    //do nothing
                    break;
                default:
                    break;
            }
        }
    }
}
