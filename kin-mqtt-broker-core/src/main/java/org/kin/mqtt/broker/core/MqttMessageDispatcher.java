package org.kin.mqtt.broker.core;

import io.micrometer.core.instrument.Metrics;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.HashedWheelTimer;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.core.cluster.BrokerManager;
import org.kin.mqtt.broker.core.event.MqttPublishEvent;
import org.kin.mqtt.broker.core.message.*;
import org.kin.mqtt.broker.core.message.handler.*;
import org.kin.mqtt.broker.core.topic.PubTopic;
import org.kin.mqtt.broker.metrics.MetricsNames;
import org.kin.mqtt.broker.utils.TopicUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.ReactorNetty;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * mqtt message分派给具体的mqtt message handler处理
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class MqttMessageDispatcher {
    private static final Logger log = LoggerFactory.getLogger(MqttMessageDispatcher.class);

    /** key -> mqtt message type, value -> mqtt message handler */
    private final Map<MqttMessageType, MqttMessageHandler<MqttMessage>> type2handler;
    /** mqtt消息处理拦截器 */
    private final List<Interceptor> interceptors;

    public MqttMessageDispatcher() {
        this(Collections.emptyList());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public MqttMessageDispatcher(List<Interceptor> interceptors) {
        Map<MqttMessageType, MqttMessageHandler<MqttMessage>> type2handler = new UnifiedMap<>();
        List<MqttMessageHandler> mqttMessageHandlers = Arrays.asList(
                new ConnAckHandler(),
                new ConnectHandler(),
                new DisconnectHandler(),
                new PingReqHandler(),
                new PingRespHandler(),
                new PubAckHandler(),
                new PubCompHandler(),
                new PublishHandler(),
                new PubRecHandler(),
                new PubRelHandler(),
                new SubAckHandler(),
                new SubscribeHandler(),
                new UnsubAckHandler(),
                new UnsubscribeHandler());
        for (MqttMessageHandler messageHandler : mqttMessageHandlers) {
            type2handler.put(messageHandler.getMqttMessageType(), messageHandler);
        }

        this.type2handler = Collections.unmodifiableMap(type2handler);
        this.interceptors = Collections.unmodifiableList(interceptors);
    }

    /**
     * mqtt消息分派处理逻辑
     *
     * @param messageContext mqtt message messageContext
     * @param brokerContext  mqtt broker context
     */
    public Mono<Void> dispatch(MqttMessageContext<? extends MqttMessage> messageContext, MqttBrokerContext brokerContext) {
        return dispatch(messageContext, null, brokerContext)
                .then(Mono.fromRunnable(() -> ReactorNetty.safeRelease(messageContext.getMessage())));
    }

    /**
     * mqtt消息分派处理逻辑
     *
     * @param messageContext mqtt message messageContext
     * @param mqttSession    mqtt session, 当处理来自于集群广播的mqtt 消息时为null
     * @param brokerContext  mqtt broker context
     */
    public Mono<Void> dispatch(MqttMessageContext<? extends MqttMessage> messageContext, @Nullable MqttSession mqttSession, MqttBrokerContext brokerContext) {
        if (!messageContext.isFromCluster()) {
            //mqtt session必定不为null
            return dispatchClientMessage(messageContext, Objects.requireNonNull(mqttSession), brokerContext);
        } else {
            return Mono.fromRunnable(() -> dispatchClusterMessage(messageContext, brokerContext));
        }
    }

    /**
     * 处理mqtt channel消息
     */
    @SuppressWarnings("unchecked")
    private Mono<Void> dispatchClientMessage(MqttMessageContext<? extends MqttMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        String clientId = mqttSession.getClientId();
        //handle mqtt message
        MqttMessage mqttMessage = messageContext.getMessage();
        if (StringUtils.isBlank(clientId) && !(mqttMessage instanceof MqttConnectMessage)) {
            //session is inactive or unregister, so force close
            //非connect验证, 就发送其他mqtt消息, 直接强制close channel
            log.warn("session '{}' is unregister, but receive non mqtt connect message, so close channel", clientId);
            return mqttSession.close();
        }

        if (mqttMessage.decoderResult().isFailure()) {
            //mqtt message decode failure, so force close
            log.warn("mqtt message from session '{}' decode failure, {}", clientId, mqttMessage);
            return mqttSession.close();
        }

        //mqtt消息预处理
        MqttMessageReplica messageReplica = null;
        if (mqttMessage instanceof MqttPublishMessage) {
            //转换成可持久化的消息
            MqttPublishMessage publishMessage = (MqttPublishMessage) mqttMessage;
            //尝试替换真实topic
            MqttPublishMessage realPublishMessage = tryReplaceRealTopic(mqttSession, publishMessage);
            if (realPublishMessage != publishMessage) {
                //replace
                mqttMessage = realPublishMessage;
                messageContext = MqttMessageContext.common(messageContext, mqttMessage);
            }

            messageReplica = MqttMessageHelper.toReplica((MqttMessageContext<MqttPublishMessage>) messageContext);
        }

        //interceptor handle
        //目前mqtt消息处理是全异步过程, 所以这里不打算使用递归形式的拦截器实现
        for (Interceptor interceptor : interceptors) {
            if (interceptor.intercept(messageContext, mqttSession, brokerContext)) {
                //intercept
                return Mono.error(new MqttBrokerException(String.format("mqtt message from session '%s' intercepted by %s, %s",
                        clientId,
                        interceptor.getClass().getName(),
                        mqttMessage)));
            }
        }

        MqttMessageReplica finalMessageReplica = messageReplica;
        return handleMqttMessage(messageContext, mqttSession, brokerContext)
                .then(Mono.fromRunnable(() -> {
                    //仅仅处理publish消息
                    if (Objects.nonNull(finalMessageReplica)) {
                        //往集群广播mqtt消息
                        BrokerManager brokerManager = brokerContext.getBrokerManager();
                        brokerManager.broadcastMqttMessage(finalMessageReplica).subscribe();

                        //规则匹配
                        //原则上所有节点都会同步rule, 那么集群广播的publish消息不需要再重复处理rule了
                        brokerContext.getRuleEngine().execute(brokerContext, finalMessageReplica).subscribe();

                        brokerContext.broadcastEvent(new MqttPublishEvent(mqttSession, finalMessageReplica));
                    }
                }));
    }

    /**
     * 尝试替换{@link MqttPublishMessage}的topic, 针对使用了topic alias场景
     *
     * @return 替换后的MqttPublishMessage实例
     */
    @SuppressWarnings("unchecked")
    private MqttPublishMessage tryReplaceRealTopic(MqttSession mqttSession, MqttPublishMessage publishMessage) {
        //因为topic alias仅在对应连接生效和维护, 所以集群广播消息需要带真实topic
        MqttFixedHeader fixedHeader = publishMessage.fixedHeader();
        MqttPublishVariableHeader publishVariableHeader = publishMessage.variableHeader();
        String topic = publishVariableHeader.topicName();
        MqttProperties properties = publishVariableHeader.properties();
        //是否需要替换mqtt消息
        boolean needReplaceMqttMessage = false;
        if (StringUtils.isBlank(topic)) {
            //topic为空, 可能使用了topic别名
            MqttProperties.MqttProperty<Integer> topicAliasProp = properties.getProperty(MqttProperties.MqttPropertyType.TOPIC_ALIAS.value());
            if (Objects.nonNull(topicAliasProp)) {
                //带了topic别名, 则尝试获取真实topic
                topic = mqttSession.getTopicByAlias(topicAliasProp.value());
                needReplaceMqttMessage = true;
            }
            if (StringUtils.isBlank(topic)) {
                //再次检查
                throw new IllegalStateException("publish message topic is blank, " + publishMessage);
            }
        } else {
            //topic不为空, 尝试注册topic别名
            MqttProperties.MqttProperty<Integer> topicAliasProp = properties.getProperty(MqttProperties.MqttPropertyType.TOPIC_ALIAS.value());
            if (Objects.nonNull(topicAliasProp)) {
                //带了topic别名, 则注册
                mqttSession.registerTopicAlias(topicAliasProp.value(), topic);
            }
        }

        if (needReplaceMqttMessage) {
            //new publish message
            publishVariableHeader = new MqttPublishVariableHeader(topic, publishVariableHeader.packetId(), properties);
            return new MqttPublishMessage(fixedHeader, publishVariableHeader, publishMessage.payload());
        }

        return publishMessage;
    }

    /**
     * 处理mqtt message
     */
    @SuppressWarnings("unchecked")
    private Mono<Void> handleMqttMessage(MqttMessageContext<? extends MqttMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        MqttMessage mqttMessage = messageContext.getMessage();
        MqttFixedHeader fixedHeader = mqttMessage.fixedHeader();
        MqttMessageType mqttMessageType = fixedHeader.messageType();
        log.debug("prepare to handle {} message from session {}", mqttMessageType, mqttSession);
        MqttMessageHandler<MqttMessage> messageHandler = type2handler.get(mqttMessageType);
        if (Objects.nonNull(messageHandler)) {
            return messageHandler.handle((MqttMessageContext<MqttMessage>) messageContext, mqttSession, brokerContext)
                    .contextWrite(context -> context.putNonNull(MqttBrokerContext.class, brokerContext))
                    .doOnError(error -> log.error("handle {} message from session {} error", mqttMessageType, mqttSession, error))
                    //释放onMqttClientConnected里面的retain(), 还有initBrokerManager的MqttMessageReplica.fromCluster(....)
                    .doOnSuccess(v -> ReactorNetty.safeRelease(mqttMessage.payload()));
        } else {
            ReactorNetty.safeRelease(mqttMessage.payload());
            return Mono.error(new IllegalArgumentException(String.format("does not find handler to handle %s message", mqttMessageType)));
        }
    }

    /**
     * 处理集群广播消息
     *
     * @return complete signal
     */
    @SuppressWarnings("unchecked")
    private Mono<Void> dispatchClusterMessage(MqttMessageContext<? extends MqttMessage> messageContext, MqttBrokerContext brokerContext) {
        MqttMessage mqttMessage = messageContext.getMessage();
        if (mqttMessage.decoderResult().isFailure()) {
            //mqtt message decode failure
            return Mono.empty();
        }

        if (!(mqttMessage instanceof MqttPublishMessage)) {
            //集群仅会广播publish消息
            return Mono.empty();
        }

        //处理publish消息
        MqttPublishMessage publishMessage = (MqttPublishMessage) mqttMessage;
        MqttPublishVariableHeader variableHeader = publishMessage.variableHeader();
        PubTopic pubTopic = TopicUtils.parsePubTopic(variableHeader.topicName());

        return MqttPublishMessageHelper.broadcast(brokerContext, pubTopic, (MqttMessageContext<MqttPublishMessage>) messageContext)
                .then(MqttPublishMessageHelper.trySaveRetainMessage(brokerContext.getMessageStore(), (MqttMessageContext<MqttPublishMessage>) messageContext))
                .then(Mono.fromRunnable(() -> Metrics.counter(MetricsNames.CLUSTER_PUBLISH_MSG_COUNT).increment()));
    }

    /**
     * 处理延迟发布publish消息
     *
     * @param brokerContext  broker context
     * @param pubTopic       publish message topic信息
     * @param messageContext publish message context
     * @return
     */
    public Mono<Void> handleDelayedPublishMessage(MqttBrokerContext brokerContext, PubTopic pubTopic,
                                                  MqttMessageContext<MqttPublishMessage> messageContext) {
        return Mono.fromRunnable(() -> {
            // TODO: 2023/4/16 delay mqtt消息为实现持久化和broker重启后重新调度
            HashedWheelTimer bsTimer = brokerContext.getBsTimer();
            //reference count+1
            //ack后payload会被touch
            messageContext.getMessage().payload().retain();
            //remove delay info
            bsTimer.newTimeout(t -> MqttPublishMessageHelper.broadcast(brokerContext, new PubTopic(pubTopic.getName()), messageContext).subscribe(),
                    pubTopic.getDelay(), TimeUnit.SECONDS);
        });
    }
}
