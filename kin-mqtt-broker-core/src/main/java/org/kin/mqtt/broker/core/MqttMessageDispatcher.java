package org.kin.mqtt.broker.core;

import io.netty.handler.codec.mqtt.*;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.cluster.BrokerManager;
import org.kin.mqtt.broker.core.message.MqttMessageHandler;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
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
     * @param wrapper       mqtt message wrapper
     * @param mqttChannel   mqtt channel
     * @param brokerContext mqtt broker context
     */
    @SuppressWarnings("unchecked")
    public void dispatch(MqttMessageWrapper<? extends MqttMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        //interceptor handle
        //目前mqtt消息处理是全异步过程, 所以这里不打算使用递归形式的拦截器实现
        for (Interceptor interceptor : interceptors) {
            if (interceptor.intercept(wrapper, mqttChannel, brokerContext)) {
                //intercept
                return;
            }
        }

        //handle mqtt message
        MqttMessage mqttMessage = wrapper.getMessage();
        MqttFixedHeader fixedHeader = mqttMessage.fixedHeader();
        MqttMessageReplica messageReplica = null;
        if (mqttMessage instanceof MqttPublishMessage) {
            //先转换成可持久化的消息
            MqttPublishMessage publishMessage = (MqttPublishMessage) mqttMessage;

            if (wrapper.isFromCluster()) {
                //非集群广播消息
                publishMessage = tryReplaceRealTopic(mqttChannel, publishMessage);
                //replace
                mqttMessage = publishMessage;
                wrapper.replaceMessage(mqttMessage);
            }

            messageReplica = MqttMessageUtils.toReplica(mqttChannel.clientId, publishMessage, wrapper.getTimestamp());
        }

        MqttMessageType mqttMessageType = fixedHeader.messageType();
        log.debug("prepare to handle {} message from channel {}", mqttMessageType, mqttChannel);
        MqttMessageHandler<MqttMessage> messageHandler = type2handler.get(mqttMessageType);
        if (Objects.nonNull(messageHandler)) {
            MqttMessage internalMqttMessage = mqttMessage;
            messageHandler.handle((MqttMessageWrapper<MqttMessage>) wrapper, mqttChannel, brokerContext)
                    .contextWrite(context -> context.putNonNull(MqttBrokerContext.class, brokerContext))
                    .subscribe(v -> {
                            },
                            error -> log.error("handle {} message from channel {} error", mqttMessageType, mqttChannel, error),
                            //释放onMqttClientConnected里面的retain(), 还有initBrokerManager的MqttMessageReplica.fromCluster(....)
                            () -> ReactorNetty.safeRelease(internalMqttMessage.payload()));
        } else {
            throw new IllegalArgumentException(String.format("does not find handler to handle %s message", mqttMessageType));
        }

        //仅仅处理publish消息
        if (Objects.nonNull(messageReplica)) {
            if (!wrapper.isFromCluster()) {
                //非集群广播消息
                //往集群广播mqtt消息
                BrokerManager brokerManager = brokerContext.getBrokerManager();
                brokerManager.broadcastMqttMessage(messageReplica).subscribe();

                //规则匹配
                //原则上所有节点都会同步rule, 那么集群广播的publish消息不需要再重复处理rule了
                brokerContext.getRuleEngine().execute(brokerContext, messageReplica).subscribe();
            }

            brokerContext.broadcastEvent(new MqttPublishEvent(mqttChannel, messageReplica));
        }
    }

    /**
     * 尝试替换{@link MqttPublishMessage}的topic, 针对使用了topic alias场景
     *
     * @return 替换后的MqttPublishMessage实例
     */
    @SuppressWarnings("unchecked")
    private MqttPublishMessage tryReplaceRealTopic(MqttChannel mqttChannel, MqttPublishMessage publishMessage) {
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
                topic = mqttChannel.getTopicByAlias(topicAliasProp.value());
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
                mqttChannel.registerTopicAlias(topicAliasProp.value(), topic);
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
