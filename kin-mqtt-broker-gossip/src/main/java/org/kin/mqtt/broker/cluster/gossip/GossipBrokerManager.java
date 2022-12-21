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
import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.reactor.event.EventConsumer;
import org.kin.framework.reactor.event.ReactorEventBus;
import org.kin.framework.utils.ClassUtils;
import org.kin.framework.utils.JSON;
import org.kin.framework.utils.NetUtils;
import org.kin.mqtt.broker.cluster.BrokerManager;
import org.kin.mqtt.broker.cluster.MqttBrokerNode;
import org.kin.mqtt.broker.cluster.event.AbstractMqttClusterEvent;
import org.kin.mqtt.broker.cluster.event.MqttClusterEvent;
import org.kin.mqtt.broker.cluster.event.SubscriptionsAddEvent;
import org.kin.mqtt.broker.cluster.event.SubscriptionsRemoveEvent;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 基于gossip集群发现机制
 *
 * @author huangjianqin
 * @date 2022/11/16
 */
public class GossipBrokerManager implements BrokerManager {
    private static final Logger log = LoggerFactory.getLogger(GossipBrokerManager.class);
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
    /** 集群节点信息, key -> host:port */
    private final Map<String, GossipNode> clusterBrokers = new NonBlockingHashMap<>();

    public GossipBrokerManager(GossipConfig config) {
        this.config = config;
    }

    @Override
    public Mono<Void> start(MqttBrokerContext brokerContext) {
        this.brokerContext = brokerContext;
        ReactorEventBus eventBus = this.brokerContext.getEventBus();
        //注册事件
        eventBus.register(new SubscriptionsAddEventConsumer());
        eventBus.register(new SubscriptionsRemoveEventConsumer());

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
        return Flux.fromIterable(clusterBrokers.values());
    }

    @Override
    public Mono<Void> broadcastMqttMessage(MqttMessageReplica message) {
        return Flux.fromIterable(clusterBrokers.values())
                //过滤没有订阅的broker节点
                .filter(n -> n.hasSubscription(message.getTopic()))
                //只往有订阅的broker节点广播publish消息
                .flatMap(n -> clusterMono.flatMap(c -> {
                    log.debug("cluster send message {} to node({}:{}:{}:{})",
                            message, n.getNamespace(), n.getName(), n.getHost(), n.getPort());
                    return c.send(Address.create(n.getHost(), n.getPort()), Message.builder()
                            .data(message)
                            .build());
                })).then();
    }

    @Override
    public Mono<Void> broadcastEvent(MqttClusterEvent event) {
        return clusterMono.flatMap(c -> {
                    log.debug("cluster broadcast event {} ", event);
                    return c.spreadGossip(Message.builder()
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
            //mqtt message
            log.debug("cluster accept message {} ", message);
            clusterMqttMessageSink.emitNext(message.data(), new RetryNonSerializedEmitFailureHandler());
        }

        @Override
        public void onGossip(Message message) {
            //mqtt event
            log.debug("cluster accept gossip message {} ", message);
            Map<String, String> headers = message.headers();
            String eventTypeStr = null;
            Class<? extends MqttClusterEvent> eventType;
            try {
                eventTypeStr = headers.getOrDefault(HEADER_MQTT_EVENT_TYPE, "");
                eventType = ClassUtils.getClass(eventTypeStr);
            } catch (Exception e) {
                throw new IllegalStateException(String.format("unknown mqtt event class '%s'", eventTypeStr));
            }

            MqttClusterEvent event = JSON.read((String) message.data(), eventType);
            if (event instanceof AbstractMqttClusterEvent) {
                ((AbstractMqttClusterEvent) event).setAddress(message.sender().toString());
            }
            brokerContext.broadcastEvent(event);
        }

        @Override
        public void onMembershipEvent(MembershipEvent event) {
            Member member = event.member();
            String namespace = member.namespace();
            String name = member.alias();
            Address memberAddress = member.address();
            String address = memberAddress.toString();
            log.info("mqtt broker(namespace:alias:address) '{}:{}:{}' {}",
                    namespace, name, address, event.type());
            switch (event.type()) {
                case ADDED:
                    clusterBrokers.put(address, GossipNode.builder()
                            .name(name)
                            .host(memberAddress.host())
                            .port(memberAddress.port())
                            .namespace(namespace)
                            .build());
                    break;
                case LEAVING:
                    clusterBrokers.remove(address);
                    break;
                case REMOVED:
                    clusterBrokers.remove(address);
                    break;
                case UPDATED:
                    //do nothing
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 注册订阅消费
     */
    private class SubscriptionsAddEventConsumer implements EventConsumer<SubscriptionsAddEvent> {

        @Override
        public void consume(ReactorEventBus eventBus, SubscriptionsAddEvent event) {
            GossipNode node = clusterBrokers.get(event.getAddress());
            node.addSubscriptions(event.getSubscriptionRegexs());
        }
    }

    /**
     * 取消注册订阅消费
     */
    private class SubscriptionsRemoveEventConsumer implements EventConsumer<SubscriptionsRemoveEvent> {

        @Override
        public void consume(ReactorEventBus eventBus, SubscriptionsRemoveEvent event) {
            GossipNode node = clusterBrokers.get(event.getAddress());
            node.removeSubscriptions(event.getSubscriptionRegexs());
        }
    }
}
