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
import org.kin.framework.reactor.event.DefaultReactorEventBus;
import org.kin.framework.reactor.event.ReactorEventBus;
import org.kin.framework.utils.ClassUtils;
import org.kin.framework.utils.CollectionUtils;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.cluster.BrokerManager;
import org.kin.mqtt.broker.cluster.MqttBrokerNode;
import org.kin.mqtt.broker.cluster.event.AbstractMqttClusterEvent;
import org.kin.mqtt.broker.cluster.event.AbstractMqttClusterEventConsumer;
import org.kin.mqtt.broker.cluster.event.MqttClusterEvent;
import org.kin.mqtt.broker.cluster.gossip.event.GossipNodeAddEvent;
import org.kin.mqtt.broker.cluster.gossip.event.TopicSubscriptionFetchEvent;
import org.kin.mqtt.broker.cluster.gossip.event.TopicSubscriptionSyncEvent;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.event.MqttEventConsumer;
import org.kin.mqtt.broker.event.MqttSubscribeEvent;
import org.kin.mqtt.broker.event.MqttUnsubscribeEvent;
import org.kin.mqtt.broker.utils.TopicUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.TimeUnit;
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
    private static final Logger log = LoggerFactory.getLogger(GossipBrokerManager.class);
    /** gossip消息header, 标识mqtt事件类型 */
    private static final String HEADER_MQTT_EVENT_TYPE = "MqttEventType";
    /** gossip消息header, 标识目标mqtt clientId */
    private static final String HEADER_MQTT_CLIENT_ID = "MqttClientId";

    /** config */
    private final GossipConfig config;
    /**
     * mqtt broker context
     * lazy init
     */
    private MqttBrokerContext brokerContext;
    /** 来自集群广播的mqtt消息流 */
    private final Sinks.Many<MqttMessageReplica> clusterMqttMessageSink = Sinks.many().unicast().onBackpressureBuffer();
    /** gossip cluster */
    private volatile Mono<Cluster> clusterMono;
    /** remote集群节点信息, key -> host:port */
    private final Map<String, GossipNode> clusterBrokers = new NonBlockingHashMap<>();
    /**
     * 本地订阅信息
     * 单线程处理, 故没有使用同步map
     * key -> 转换成正则后的topic name
     */
    private final Map<String, LocalTopicSubscription> localTopicSubscriptionMap = new HashMap<>();
    /** 集群broker间订阅同步事件总线 */
    private final ReactorEventBus subscriptionSyncEventBus;
    /** 定时强制同步本节点订阅信息{@link  Disposable}实例 */
    private final Disposable forceSyncSubscriptionDisposable;

    public GossipBrokerManager(GossipConfig config) {
        this.config = config;
        Scheduler syncSubscriptionScheduler = Schedulers.newSingle(config.getNamespace() + "-subscription-sync");
        this.subscriptionSyncEventBus = new DefaultReactorEventBus(true, syncSubscriptionScheduler);
        long syncSubscriptionMills = config.getSyncSubscriptionMills();
        if (syncSubscriptionMills < 0) {
            syncSubscriptionMills = GossipConfig.DEFAULT_SYNC_SUBSCRIPTION_MILLS;
        }
        //定时强制同步本节点订阅信息, 即哪怕出错了, 最晚n毫秒后, 及时更新
        forceSyncSubscriptionDisposable = syncSubscriptionScheduler.schedulePeriodically(this::forceSyncTopicSubscription,
                syncSubscriptionMills, syncSubscriptionMills, TimeUnit.MILLISECONDS);
    }

    @Override
    public Mono<Void> start(MqttBrokerContext brokerContext) {
        this.brokerContext = brokerContext;
        ReactorEventBus eventBus = this.brokerContext.getEventBus();
        //注册事件
        eventBus.register(new MqttSubscribeEventConsumer());
        eventBus.register(new MqttUnsubscribeEventConsumer());

        //集群broker间订阅同步事件
        subscriptionSyncEventBus.register(new LocalMqttSubscribeEventConsumer());
        subscriptionSyncEventBus.register(new LocalMqttUnsubscribeEventConsumer());
        subscriptionSyncEventBus.register(new TopicSubscriptionSyncEventConsumer());
        subscriptionSyncEventBus.register(new GossipNodeAddEventConsumer());
        subscriptionSyncEventBus.register(new TopicSubscriptionFetchEventConsumer());

        int port = config.getPort();
        clusterMono = new ClusterImpl().config(clusterConfig -> clusterConfig.externalHost(config.getHost())
                        //gossip节点名即broker id
                        .memberAlias(brokerContext.getBrokerId())
                        .externalPort(port))
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
    public Mono<Void> sendMqttMessage(String remoteBrokerId, String clientId, MqttMessageReplica message) {
        return Flux.fromIterable(clusterBrokers.values())
                .filter(n -> n.getName().equals(remoteBrokerId))
                .flatMap(n -> clusterMono.flatMap(c -> {
                    log.debug("cluster send message {} to node({}:{}:{}:{})",
                            message, n.getNamespace(), n.getName(), n.getHost(), n.getPort());
                    return c.send(Address.create(n.getHost(), n.getPort()), Message.builder()
                            .header(HEADER_MQTT_CLIENT_ID, clientId)
                            .data(message)
                            .build());
                })).then();
    }

    @Override
    public Mono<Void> broadcastEvent(MqttClusterEvent event) {
        if (event instanceof AbstractMqttClusterEvent) {
            ((AbstractMqttClusterEvent) event).setAddress(localAddress());
        }
        return clusterMono.flatMap(c -> {
                    log.debug("cluster broadcast event {} ", event);
                    return c.spreadGossip(Message.builder()
                            .header(HEADER_MQTT_EVENT_TYPE, event.getClass().getName())
                            .data(JSON.write(event))
                            .build());
                })
                .then();
    }

    /**
     * 获取gossip节点address
     */
    private String localAddress() {
        return config.getHost() + ":" + config.getPort();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends MqttBrokerNode> T getNode(String address) {
        return (T) clusterBrokers.get(address);
    }

    @Override
    public Mono<Void> shutdown() {
        return Mono.fromRunnable(forceSyncSubscriptionDisposable::dispose)
                .flatMap(v -> clusterMono.flatMap(cluster -> {
                    //关联gossip cluster shutdown后的操作
                    cluster.onShutdown()
                            //close sink
                            .then(Mono.fromRunnable(() -> clusterMqttMessageSink.emitComplete(RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED)))
                            //close subscription sync
                            .then(Mono.fromRunnable(subscriptionSyncEventBus::dispose))
                            .subscribe();

                    return Mono.fromRunnable(cluster::shutdown);
                }));
    }

    /**
     * 主动向集群中broker节点同步更新自己的订阅信息
     * {@link #subscriptionSyncEventBus}单线程处理
     */
    private void forceSyncTopicSubscription() {
        if (CollectionUtils.isEmpty(localTopicSubscriptionMap)) {
            return;
        }

        //本broker节点订阅信息
        List<RemoteTopicSubscription> changedSubscriptions = new ArrayList<>();
        for (LocalTopicSubscription subscription : localTopicSubscriptionMap.values()) {
            changedSubscriptions.add(new RemoteTopicSubscription(subscription.getRegexTopic())
                    .tid(subscription.getTid())
                    .subscribed(subscription.isSubscribed()));
        }

        broadcastEvent(new TopicSubscriptionSyncEvent(changedSubscriptions)).subscribe();
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
            Map<String, String> headers = message.headers();
            if (!headers.containsKey(HEADER_MQTT_EVENT_TYPE)) {
                //mqtt publish消息
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
                    mqttSession.sendMessage(MqttMessageHelper.createPublish(messageReplica), messageReplica.getQos() > 0)
                            .subscribe();
                }
            } else {
                //mqtt event
                subscriptionSyncEventBus.post((MqttClusterEvent) message.data());
            }
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

            MqttClusterEvent mqttClusterEvent = JSON.read((String) message.data(), eventType);
            if (mqttClusterEvent instanceof TopicSubscriptionSyncEvent) {
                //gossip节点间同步mqtt broker订阅事件
                subscriptionSyncEventBus.post(mqttClusterEvent);
            } else {
                brokerContext.broadcastEvent(mqttClusterEvent);
            }
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
                    subscriptionSyncEventBus.post(new GossipNodeAddEvent(memberAddress));
                    break;
                case LEAVING:
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

    //--------------------------------------------------------local mqtt event consumer

    /**
     * 注册订阅consumer
     */
    private class MqttSubscribeEventConsumer implements MqttEventConsumer<MqttSubscribeEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, MqttSubscribeEvent event) {
            subscriptionSyncEventBus.post(event);
        }
    }

    /**
     * 取消订阅consumer
     */
    private class MqttUnsubscribeEventConsumer implements MqttEventConsumer<MqttUnsubscribeEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, MqttUnsubscribeEvent event) {
            subscriptionSyncEventBus.post(event);
        }
    }

    //--------------------------------------------------------集群broker间订阅同步事件consumer

    /**
     * 注册订阅时, 集群节点同步订阅处理
     * {@link #subscriptionSyncEventBus}单线程处理
     */
    private class LocalMqttSubscribeEventConsumer implements MqttEventConsumer<MqttSubscribeEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, MqttSubscribeEvent event) {
            List<RemoteTopicSubscription> changedSubscriptions = new ArrayList<>();
            for (TopicSubscription subscription : event.getSubscriptions()) {
                String topic = subscription.getTopic();
                String regexTopic = TopicUtils.toRegexTopic(topic);
                LocalTopicSubscription localTopicSubscription = localTopicSubscriptionMap.computeIfAbsent(regexTopic, LocalTopicSubscription::new);

                long tid = localTopicSubscription.onSubscribe();
                if (tid < 0) {
                    continue;
                }

                //首次订阅才广播订阅变化
                changedSubscriptions.add(new RemoteTopicSubscription(regexTopic).tid(tid).subscribed(true));
            }

            if (CollectionUtils.isEmpty(changedSubscriptions)) {
                return;
            }

            broadcastEvent(new TopicSubscriptionSyncEvent(changedSubscriptions)).subscribe();
        }
    }

    /**
     * 取消订阅时, 集群节点同步订阅处理
     * {@link #subscriptionSyncEventBus}单线程处理
     */
    private class LocalMqttUnsubscribeEventConsumer implements MqttEventConsumer<MqttUnsubscribeEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, MqttUnsubscribeEvent event) {
            List<RemoteTopicSubscription> changedSubscriptions = new ArrayList<>();
            for (String topic : event.getTopics()) {
                String regexTopic = TopicUtils.toRegexTopic(topic);
                LocalTopicSubscription localTopicSubscription = localTopicSubscriptionMap.computeIfAbsent(regexTopic, LocalTopicSubscription::new);

                long tid = localTopicSubscription.onUnsubscribe();
                if (tid < 0) {
                    continue;
                }

                //没有任何订阅才广播订阅变化
                changedSubscriptions.add(new RemoteTopicSubscription(regexTopic).tid(tid).subscribed(false));
            }

            if (CollectionUtils.isEmpty(changedSubscriptions)) {
                return;
            }

            broadcastEvent(new TopicSubscriptionSyncEvent(changedSubscriptions)).subscribe();
        }
    }

    /**
     * 集群broker间订阅同步事件consumer
     * {@link #subscriptionSyncEventBus}单线程处理
     */
    private class TopicSubscriptionSyncEventConsumer extends AbstractMqttClusterEventConsumer<TopicSubscriptionSyncEvent, GossipNode> {
        TopicSubscriptionSyncEventConsumer() {
            super(GossipBrokerManager.this);
        }

        @Override
        protected void consume(ReactorEventBus eventBus, GossipNode node, TopicSubscriptionSyncEvent event) {
            log.debug("before sync topic subscription, {}", node);

            //remote mqtt broker订阅变化时, 维护该节点的订阅信息
            node.onSubscriptionChanged(event.getChangedSubscriptions());

            log.debug("after sync topic subscription, {}", node);
        }
    }

    /**
     * 新节点上线, 发送fetch事件
     * {@link #subscriptionSyncEventBus}单线程处理
     */
    private class GossipNodeAddEventConsumer implements MqttEventConsumer<GossipNodeAddEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, GossipNodeAddEvent event) {
            //新节点address
            Address address = event.getMemberAddress();

            List<RemoteTopicSubscription> changedSubscriptions = new ArrayList<>();
            for (LocalTopicSubscription subscription : localTopicSubscriptionMap.values()) {
                changedSubscriptions.add(new RemoteTopicSubscription(subscription.getRegexTopic())
                        .tid(subscription.getTid())
                        .subscribed(subscription.isSubscribed()));
            }

            //新节点上线, 发送fetch事件
            Message message = Message.builder()
                    .data(new TopicSubscriptionFetchEvent(localAddress()))
                    .header(HEADER_MQTT_EVENT_TYPE, event.getClass().getName())
                    .build();
            //直接send, 没有通过gossip协议发送
            clusterMono.flatMap(c -> c.send(address, message))
                    .subscribe();
        }
    }

    /**
     * 接收到fetch请求, 返回本broker节点订阅信息
     * {@link #subscriptionSyncEventBus}单线程处理
     */
    private class TopicSubscriptionFetchEventConsumer implements MqttEventConsumer<TopicSubscriptionFetchEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, TopicSubscriptionFetchEvent event) {
            log.debug("receive fetch topic subscription event from node '{}'", event.getAddress());

            //请求节点address
            Address address = Address.from(event.getAddress());

            //本broker节点订阅信息
            List<RemoteTopicSubscription> changedSubscriptions = new ArrayList<>();
            for (LocalTopicSubscription subscription : localTopicSubscriptionMap.values()) {
                changedSubscriptions.add(new RemoteTopicSubscription(subscription.getRegexTopic())
                        .tid(subscription.getTid())
                        .subscribed(subscription.isSubscribed()));
            }

            //message
            Message message = Message.builder()
                    .data(new TopicSubscriptionSyncEvent(localAddress(), changedSubscriptions))
                    .header(HEADER_MQTT_EVENT_TYPE, event.getClass().getName())
                    .build();

            //直接send, 没有通过gossip协议发送
            clusterMono.flatMap(c -> c.send(address, message))
                    .subscribe();
        }
    }
}
