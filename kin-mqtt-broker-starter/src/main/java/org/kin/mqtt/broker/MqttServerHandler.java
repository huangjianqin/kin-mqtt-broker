package org.kin.mqtt.broker;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * mqtt netty handler
 *
 * @author huangjianqin
 * @date 2022/11/6
 */
final class MqttServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(MqttServerHandler.class);
    /** 与netty channel绑定的mqtt session */
    private static final AttributeKey<MqttSession> MQTT_SESSION_ATTRIBUTE_KEY = AttributeKey.newInstance("mqtt_session");

    /**
     * mqtt消息交互入口
     *
     * @param ctx channel handler context
     * @param msg MQTT message
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        MqttMessage message = (MqttMessage) msg;
        MqttMessageType mqttMessageType = message.fixedHeader().messageType();
        switch (mqttMessageType) {
            case CONNECT:
                MqttConnAckMessage connAckMessage = onConnect((MqttConnectMessage) message, ctx);
                ctx.writeAndFlush(connAckMessage);
                break;
            case PUBLISH:
                MqttPubAckMessage pubAckMessage = onPublish((MqttPublishMessage) message, ctx);
                ctx.writeAndFlush(pubAckMessage);
                break;
            case PUBACK:
                onPubAck((MqttPubAckMessage) message, ctx);
                break;
            case SUBSCRIBE:
                MqttSubAckMessage subAckMessage = onSubscribe((MqttSubscribeMessage) message, ctx);
                ctx.writeAndFlush(subAckMessage);
                break;
            case UNSUBSCRIBE:
                MqttUnsubAckMessage unsubAckMessage = onUnSubscribe((MqttUnsubscribeMessage) message, ctx);
                ctx.writeAndFlush(unsubAckMessage);
                break;
            case PINGREQ:
                MqttMessage pingResp = onPing();
                ctx.writeAndFlush(pingResp);
                break;
            case DISCONNECT:
                onDisconnect(ctx);
                break;
            default:
                log.warn("do not handle '{}' message!!!", mqttMessageType.value());
                break;
        }
    }

    /**
     * 获取与netty channel绑定的mqtt session
     *
     * @param ctx netty context
     * @return mqtt session
     */
    private MqttSession getMqttSession(ChannelHandlerContext ctx) {
        Attribute<MqttSession> attr = ctx.channel().attr(MQTT_SESSION_ATTRIBUTE_KEY);
        return attr.get();
    }

    /**
     * 获取与netty channel绑定的mqtt session, 如果为null, 则抛异常
     *
     * @param ctx netty context
     * @return mqtt session
     */
    private MqttSession getMqttSessionOrThrow(ChannelHandlerContext ctx) {
        MqttSession session = getMqttSession(ctx);
        if (Objects.isNull(session)) {
            throw new IllegalStateException("mqtt connect is not established");
        }
        return session;
    }

    /**
     * 将mqtt session与之netty channel绑定
     *
     * @param ctx     netty context
     * @param session mqtt session
     */
    private void bindMqttSession(ChannelHandlerContext ctx, MqttSession session) {
        Attribute<MqttSession> attr = ctx.channel().attr(MQTT_SESSION_ATTRIBUTE_KEY);
        attr.set(session);
    }

    /**
     * 处理{@link MqttMessageType#CONNECT}消息入口
     *
     * @param mqttMessage {@link MqttMessageType#CONNECT}消息
     * @param ctx         netty context
     * @return {@link MqttMessageType#CONNACK}消息
     */
    private MqttConnAckMessage onConnect(MqttConnectMessage mqttMessage, ChannelHandlerContext ctx) {
        if (Objects.nonNull(getMqttSession(ctx))) {
            throw new IllegalStateException("connect error, mqtt session has bean created");
        }

        MqttConnectVariableHeader variableHeader = mqttMessage.variableHeader();
        String clientId = mqttMessage.payload().clientIdentifier();
        //keep alive
        int keepAlive = variableHeader.keepAliveTimeSeconds();
        ctx.pipeline().addBefore(
                "MqttServerHandler",
                "MqttIdleHandler",
                new IdleStateHandler(keepAlive, 0, 0));

        //绑定mqtt session
        MqttSession session = new MqttSession(clientId);
        bindMqttSession(ctx, session);
        MqttSessionManager.INSTANCE.register(session);

        //ack消息
        // TODO: 2022/11/7 retain length
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.FAILURE, false, 0x02);
        MqttConnAckVariableHeader connAckVariableHeader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
        return new MqttConnAckMessage(fixedHeader, connAckVariableHeader);
    }

    /**
     * 处理{@link MqttMessageType#PUBLISH}消息入口
     *
     * @param message {@link MqttMessageType#PUBLISH}消息
     * @param ctx     netty context
     * @return {@link MqttMessageType#PUBACK}消息
     */
    private MqttPubAckMessage onPublish(MqttPublishMessage message, ChannelHandlerContext ctx) {
        getMqttSession(ctx);

        MqttPublishVariableHeader variableHeader = message.variableHeader();
        //topic
        String topicName = variableHeader.topicName();
        //publish
        PublishService.INSTANCE.publish(topicName, message.payload());
        //publish ack
        // TODO: 2022/11/7 retain length
        MqttFixedHeader ackFixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_LEAST_ONCE, false, 2);
        MqttMessageIdVariableHeader messageIdVariableHeader = MqttMessageIdVariableHeader.from(Math.abs(message.variableHeader().packetId()));
        return new MqttPubAckMessage(ackFixedHeader, messageIdVariableHeader);
    }

    /**
     * 处理{@link MqttMessageType#PUBACK}消息入口
     *
     * @param message {@link MqttMessageType#PUBACK}消息
     * @param ctx     netty context
     */
    private void onPubAck(MqttPubAckMessage message, ChannelHandlerContext ctx) {
        // TODO: server publish消息收到ack处理
        System.out.println("pub ack");
    }

    /**
     * 处理{@link MqttMessageType#SUBSCRIBE}消息入口
     *
     * @param message {@link MqttMessageType#SUBSCRIBE}消息
     * @param ctx     netty context
     * @return {@link MqttMessageType#SUBACK}消息
     */
    private MqttSubAckMessage onSubscribe(MqttSubscribeMessage message, ChannelHandlerContext ctx) {
        MqttSession session = getMqttSession(ctx);

        MqttMessageIdVariableHeader subMsgVariableHeader = message.variableHeader();
        List<MqttTopicSubscription> subscriptions = message.payload().topicSubscriptions();
        List<Integer> qosList = new LinkedList<>();

        //subscribe
        for (MqttTopicSubscription topicSubscription : subscriptions) {
            if (!session.getSubscriptionManager().subscribe(ctx.channel(), topicSubscription.topicName())) {
                continue;
            }

            MqttQoS mqttQoS = topicSubscription.qualityOfService();
            qosList.add(mqttQoS.value());
        }

        //subscribe ack
        // TODO: 2022/11/7 qosList
        // TODO: 2022/11/7 retain length
        MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(qosList);
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, subMsgVariableHeader.messageId());
        return new MqttSubAckMessage(mqttFixedHeader, subMsgVariableHeader, mqttSubAckPayload);
    }

    /**
     * 处理{@link MqttMessageType#UNSUBSCRIBE}消息入口
     *
     * @param message {@link MqttMessageType#UNSUBSCRIBE}消息
     * @param ctx     netty context
     * @return {@link MqttMessageType#UNSUBACK}消息
     */
    private MqttUnsubAckMessage onUnSubscribe(MqttUnsubscribeMessage message, ChannelHandlerContext ctx) {
        MqttSession session = getMqttSessionOrThrow(ctx);

        //unsubscribe
        for (String topicName : message.payload().topics()) {
            session.getSubscriptionManager().unsubscribe(topicName);
        }

        //unsubscribe ack
        // TODO: 2022/11/7 retain length
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(message.variableHeader().messageId());
        return new MqttUnsubAckMessage(fixedHeader, variableHeader);
    }

    /**
     * 处理{@link MqttMessageType#PINGREQ}心跳消息入口
     * @return {@link MqttMessageType#PINGRESP}消息
     */
    private MqttMessage onPing() {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(mqttFixedHeader);
    }

    /**
     * 处理{@link MqttMessageType#DISCONNECT}消息入口
     */
    private void onDisconnect(ChannelHandlerContext ctx) {
        MqttSession session = getMqttSessionOrThrow(ctx);
        MqttSessionManager.INSTANCE.unregister(session.getUuid());
        session.close();
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx){
        onDisconnect(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage());
        // TODO: 2022/11/6 是否需要直接关channel
        ctx.close();
    }

}
