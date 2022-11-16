package org.kin.mqtt.broker.core;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.kin.mqtt.broker.core.cluster.BrokerManager;
import org.kin.mqtt.broker.core.message.MqttMessageHandler;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.message.handler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * mqtt message分派给具体的mqtt message handler处理
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public final class MqttMessageDispatcher {
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
        //todo 思考一下没有办法减少字节复制
        MqttMessageReplica messageReplica = null;
        if (mqttMessage instanceof MqttPublishMessage) {
            //先转换成可持久化的消息
            messageReplica = MqttMessageReplica.fromPublishMessage(mqttChannel.getClientId(), (MqttPublishMessage) mqttMessage, wrapper.getTimestamp());
        }

        MqttMessageType mqttMessageType = fixedHeader.messageType();
        String threadName = Thread.currentThread().getName();
        log.debug("{}: prepare to handle {} message from channel {}", threadName, mqttMessageType, mqttChannel);
        MqttMessageHandler<MqttMessage> messageHandler = type2handler.get(mqttMessageType);
        if (Objects.nonNull(messageHandler)) {
            messageHandler.handle((MqttMessageWrapper<MqttMessage>) wrapper, mqttChannel, brokerContext)
                    .contextWrite(context -> context.putNonNull(MqttBrokerContext.class, brokerContext))
                    .subscribeOn(MqttBrokerContext.MQTT_MESSAGE_HANDLE_SCHEDULER)
                    .subscribe(v -> {
                    }, error -> log.error("{}: handle {} message from channel {} error, {}", threadName, mqttMessageType, mqttChannel, error));
        } else {
            throw new IllegalArgumentException(String.format("does not find handler to handle %s message", mqttMessageType));
        }

        //仅仅处理publish消息
        if ((!wrapper.isFromCluster()) && Objects.nonNull(messageReplica)) {
            //集群广播mqtt消息
            BrokerManager brokerManager = brokerContext.getBrokerManager();
            brokerManager.broadcastMqttMessage(messageReplica).subscribeOn(MqttBrokerContext.MQTT_MESSAGE_HANDLE_SCHEDULER).subscribe();

            // TODO: 2022/11/14 dsl规则匹配, 支持定义广播mqtt到指定datasource
        }
    }
}
