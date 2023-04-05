package org.kin.mqtt.broker.core;

import io.netty.handler.codec.mqtt.*;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.cluster.BrokerManager;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageHandler;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.handler.*;
import org.kin.mqtt.broker.event.MqttPublishEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.ReactorNetty;

import java.util.*;

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
     * @param mqttSession    mqtt session
     * @param brokerContext  mqtt broker context
     */
    public void dispatch(MqttMessageContext<? extends MqttMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        if (!messageContext.isFromCluster()) {
            dispatchClientMessage(messageContext, mqttSession, brokerContext);
        } else {
            dispatchClusterMessage(messageContext, mqttSession, brokerContext);
        }
    }

    /**
     * 处理mqtt channel消息
     */
    private void dispatchClientMessage(MqttMessageContext<? extends MqttMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        String clientId = mqttSession.clientId;
        if (!mqttSession.isChannelActive() || mqttSession.isOffline()) {
            //session is inactive or unregister, so force close
            log.warn("session '{}' is unregister, but still receive mqtt message, so close channel", clientId);
            mqttSession.close().subscribe();
            return;
        }

        //handle mqtt message
        MqttMessage mqttMessage = messageContext.getMessage();
        if (mqttMessage.decoderResult().isFailure()) {
            //mqtt message decode failure, so force close
            log.warn("mqtt message from session '{}' decode failure, {}", clientId, mqttMessage);
            mqttSession.close().subscribe();
            return;
        }

        //interceptor handle
        //目前mqtt消息处理是全异步过程, 所以这里不打算使用递归形式的拦截器实现
        for (Interceptor interceptor : interceptors) {
            if (interceptor.intercept(messageContext, mqttSession, brokerContext)) {
                //intercept
                return;
            }
        }

        MqttMessageReplica messageReplica = null;
        if (mqttMessage instanceof MqttPublishMessage) {
            //转换成可持久化的消息
            MqttPublishMessage publishMessage = (MqttPublishMessage) mqttMessage;
            messageReplica = MqttMessageUtils.toReplica(clientId, publishMessage, messageContext.getTimestamp());
        }

        handleMqttMessage(messageContext, mqttSession, brokerContext);

        //仅仅处理publish消息
        if (Objects.nonNull(messageReplica)) {
            //往集群广播mqtt消息
            BrokerManager brokerManager = brokerContext.getBrokerManager();
            brokerManager.broadcastMqttMessage(messageReplica).subscribe();

            //规则匹配
            //原则上所有节点都会同步rule, 那么集群广播的publish消息不需要再重复处理rule了
            brokerContext.getRuleEngine().execute(brokerContext, messageReplica).subscribe();

            brokerContext.broadcastEvent(new MqttPublishEvent(mqttSession, messageReplica));
        }
    }

    /**
     * 处理集群广播消息
     */
    private void dispatchClusterMessage(MqttMessageContext<? extends MqttMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        MqttMessage mqttMessage = messageContext.getMessage();
        if (mqttMessage.decoderResult().isFailure()) {
            //mqtt message decode failure
            return;
        }

        if (!(mqttMessage instanceof MqttPublishMessage)) {
            //集群仅会广播publish消息
            return;
        }

        //interceptor handle
        //目前mqtt消息处理是全异步过程, 所以这里不打算使用递归形式的拦截器实现
        for (Interceptor interceptor : interceptors) {
            if (interceptor.intercept(messageContext, mqttSession, brokerContext)) {
                //intercept
                return;
            }
        }

        //转换成可持久化的消息
        MqttPublishMessage publishMessage = (MqttPublishMessage) mqttMessage;
        MqttMessageReplica messageReplica;
        //尝试替换真实topic
        publishMessage = tryReplaceRealTopic(mqttSession, publishMessage);
        //replace
        mqttMessage = publishMessage;
        messageContext.replaceMessage(mqttMessage);
        //副本
        messageReplica = MqttMessageUtils.toReplica(mqttSession.clientId, publishMessage, messageContext.getTimestamp());

        //处理消息
        handleMqttMessage(messageContext, mqttSession, brokerContext);

        brokerContext.broadcastEvent(new MqttPublishEvent(mqttSession, messageReplica));
    }

    /**
     * 处理mqtt message
     */
    @SuppressWarnings("unchecked")
    private void handleMqttMessage(MqttMessageContext<? extends MqttMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        MqttMessage mqttMessage = messageContext.getMessage();
        MqttFixedHeader fixedHeader = mqttMessage.fixedHeader();
        MqttMessageType mqttMessageType = fixedHeader.messageType();
        log.debug("prepare to handle {} message from session {}", mqttMessageType, mqttSession);
        MqttMessageHandler<MqttMessage> messageHandler = type2handler.get(mqttMessageType);
        if (Objects.nonNull(messageHandler)) {
            MqttMessage internalMqttMessage = mqttMessage;
            messageHandler.handle((MqttMessageContext<MqttMessage>) messageContext, mqttSession, brokerContext)
                    .contextWrite(context -> context.putNonNull(MqttBrokerContext.class, brokerContext))
                    .subscribe(v -> {
                            },
                            error -> log.error("handle {} message from session {} error", mqttMessageType, mqttSession, error),
                            //释放onMqttClientConnected里面的retain(), 还有initBrokerManager的MqttMessageReplica.fromCluster(....)
                            () -> ReactorNetty.safeRelease(internalMqttMessage.payload()));
        } else {
            throw new IllegalArgumentException(String.format("does not find handler to handle %s message", mqttMessageType));
        }
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
}
