package org.kin.mqtt.broker.core.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.mqtt.*;
import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.core.MqttChannel;

import java.util.*;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.*;

/**
 * @author huangjianqin
 * @date 2022/11/14
 */
public final class MqttMessageUtils {
    private MqttMessageUtils() {
    }

    /**
     * 构建pong消息
     *
     * @return pong消息
     */
    public static MqttMessage createPingResp() {
        return new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));
    }

    /**
     * 将{@link  Map}转换成{@link  MqttProperties}
     *
     * @param properties mqtt属性, map形式
     * @return mqtt properties
     */
    public static MqttProperties genMqttProperties(Map<String, String> properties) {
        MqttProperties mqttProperties = MqttProperties.NO_PROPERTIES;
        if (CollectionUtils.isNonEmpty(properties)) {
            mqttProperties = new MqttProperties();
            MqttProperties.UserProperties userProperties = new MqttProperties.UserProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                userProperties.add(entry.getKey(), entry.getValue());
            }
            mqttProperties.add(userProperties);
        }
        return mqttProperties;
    }

    /**
     * 将{@link  MqttProperties}转换成{@link  Map}
     *
     * @param mqttProperties mqtt属性
     * @return mqtt properties, map形式
     */
    @SuppressWarnings("rawtypes")
    public static Map<String, String> toStringProperties(MqttProperties mqttProperties) {
        if (mqttProperties == MqttProperties.NO_PROPERTIES) {
            return Collections.emptyMap();
        } else {
            Collection<? extends MqttProperties.MqttProperty> mqttPropertyList = mqttProperties.listAll();
            Map<String, String> properties = new HashMap<>(mqttPropertyList.size());
            for (MqttProperties.MqttProperty property : mqttPropertyList) {
                properties.put(String.valueOf(property.propertyId()), property.value().toString());
            }
            return properties;
        }
    }

    /**
     * 构建publish消息
     *
     * @return publish消息
     */
    public static MqttPublishMessage createPublish(boolean isDup, MqttQoS qoS, int messageId, String topic, ByteBuf message, MqttProperties properties) {
        return createPublish(isDup, qoS, false, messageId, topic, message, properties);
    }

    /**
     * 构建publish消息
     *
     * @return publish消息
     */
    public static MqttPublishMessage createPublish(boolean isDup, MqttQoS qoS, int messageId, String topic, ByteBuf message, Map<String, String> userPropertiesMap) {
        return createPublish(isDup, qoS, false, messageId, topic, message, genMqttProperties(userPropertiesMap));
    }

    /**
     * 构建publish消息
     *
     * @return publish消息
     */
    public static MqttPublishMessage createPublish(boolean isDup, MqttQoS qoS, int messageId, String topic, ByteBuf message) {
        return createPublish(isDup, qoS, false, messageId, topic, message, MqttProperties.NO_PROPERTIES);
    }

    /**
     * 构建publish消息
     *
     * @return publish消息
     */
    public static MqttPublishMessage createPublish(boolean isDup, MqttQoS qoS, boolean isRetain, int messageId, String topic, ByteBuf message) {
        return createPublish(isDup, qoS, isRetain, messageId, topic, message, MqttProperties.NO_PROPERTIES);
    }

    /**
     * 构建publish消息
     *
     * @return publish消息
     */
    public static MqttPublishMessage createPublish(boolean isDup, MqttQoS qoS, boolean isRetain, int messageId, String topic, ByteBuf message, MqttProperties properties) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, isDup, qoS, isRetain, 0);
        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topic, messageId, properties);
        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, message);
    }

    /**
     * 构建publish消息
     *
     * @return publish消息
     */
    public static MqttPublishMessage createPublish(MqttChannel mqttChannel, MqttMessageReplica replica) {
        int qos = replica.getQos();
        return createPublish(false,
                MqttQoS.valueOf(qos),
                qos > 0 ? mqttChannel.nextMessageId() : 0,
                replica.getTopic(),
                PooledByteBufAllocator.DEFAULT.directBuffer().writeBytes(replica.getPayload()),
                replica.getProperties());
    }

    /**
     * 包装publish消息
     *
     * @param messageId 消息id
     * @param message   {@link MqttPublishMessage}
     * @param mqttQoS   {@link MqttQoS}
     * @return {@link MqttPublishMessage}
     */
    public static MqttPublishMessage wrapPublish(MqttPublishMessage message, MqttQoS mqttQoS, int messageId) {
        //原message header
        MqttPublishVariableHeader variableHeader = message.variableHeader();
        MqttFixedHeader fixedHeader = message.fixedHeader();

        //new message header
        MqttFixedHeader newFixedHeader = new MqttFixedHeader(fixedHeader.messageType(), false, mqttQoS, false, fixedHeader.remainingLength());
        MqttPublishVariableHeader newVariableHeader = new MqttPublishVariableHeader(variableHeader.topicName(), messageId, variableHeader.properties());
        // TODO: 2022/11/14 copy
        return new MqttPublishMessage(newFixedHeader, newVariableHeader, message.payload().copy());
    }

    /**
     * 构建publish消息
     *
     * @return publish消息
     */
    public static MqttPublishMessage createPublish(MqttMessageReplica replica) {
        return MqttMessageUtils.createPublish(false,
                MqttQoS.valueOf(replica.getQos()),
                0,
                replica.getTopic(),
                PooledByteBufAllocator.DEFAULT.buffer().writeBytes(replica.getPayload()),
                replica.getProperties());
    }

    /**
     * 构建publish消息
     *
     * @return publish消息
     */
    public static MqttPublishMessage createPublish(MqttMessageReplica replica, String realTopic) {
        return MqttMessageUtils.createPublish(false,
                MqttQoS.valueOf(replica.getQos()),
                0,
                replica.getTopic(),
                PooledByteBufAllocator.DEFAULT.buffer().writeBytes(replica.getPayload()),
                replica.getProperties());
    }

    /**
     * 构建pub ack消息
     *
     * @return pub ack消息
     */
    public static MqttPubAckMessage createPubAck(int messageId) {
        return createPubAck(MqttMessageType.PUBACK, messageId);
    }

    /**
     * 构建pub rec消息
     *
     * @return pub rec消息
     */
    public static MqttPubAckMessage createPubRec(int messageId) {
        return createPubAck(MqttMessageType.PUBREC, messageId);
    }

    /**
     * 构建pub comp消息
     *
     * @return pub comp消息
     */
    public static MqttPubAckMessage createPubComp(int messageId) {
        return createPubAck(MqttMessageType.PUBCOMP, messageId);
    }

    /**
     * 构建ack相关消息
     *
     * @return ack相关消息
     */
    private static MqttPubAckMessage createPubAck(MqttMessageType mqttMessageType, int messageId) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(mqttMessageType, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(messageId);
        return new MqttPubAckMessage(mqttFixedHeader, variableHeader);
    }

    /**
     * 构建pub rel消息
     *
     * @return pub rel消息
     */
    public static MqttPubAckMessage createPubRel(int messageId) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0x02);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(messageId);
        return new MqttPubAckMessage(mqttFixedHeader, variableHeader);
    }

    /**
     * 构建unsub ack消息
     *
     * @return unsub ack消息
     */
    public static MqttUnsubAckMessage createUnsubAck(int messageId) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(messageId);
        return new MqttUnsubAckMessage(mqttFixedHeader, variableHeader);
    }

    /**
     * 构建sub ack消息
     *
     * @param qos Granted Qos 被取代为 Reason Code, Reason Code 中有状态码表示了具体的Granted Qos
     * @return sub ack消息
     */
    public static MqttSubAckMessage createSubAck(int messageId, List<Integer> qos) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(messageId);
        MqttSubAckPayload payload = new MqttSubAckPayload(qos);
        return new MqttSubAckMessage(mqttFixedHeader, variableHeader, payload);
    }

    /**
     * 构建conn ack消息
     *
     * @return conn ack消息
     */
    public static MqttConnAckMessage createConnAck(MqttConnectReturnCode connectReturnCode, byte version) {
        MqttProperties properties = MqttProperties.NO_PROPERTIES;
        if (MqttVersion.MQTT_5.protocolLevel() == version) {
            properties = new MqttProperties();
            // support retain msg
            properties.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RETAIN_AVAILABLE.value(), 1));
            // don't support shared subscription
            properties.add(new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.SHARED_SUBSCRIPTION_AVAILABLE.value(), 0));
            // mqtt3.0 error code transform
            switch (connectReturnCode) {
                case CONNECTION_REFUSED_IDENTIFIER_REJECTED:
                    connectReturnCode = CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
                    break;
                case CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION:
                    connectReturnCode = CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION;
                    break;
                case CONNECTION_REFUSED_SERVER_UNAVAILABLE:
                    connectReturnCode = CONNECTION_REFUSED_SERVER_UNAVAILABLE_5;
                    break;
                case CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD:
                    connectReturnCode = CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD;
                    break;
                case CONNECTION_REFUSED_NOT_AUTHORIZED:
                    connectReturnCode = CONNECTION_REFUSED_NOT_AUTHORIZED_5;
                    break;
                default:
                    //do nothing
            }
        }
        MqttConnAckVariableHeader mqttConnAckVariableHeader = new MqttConnAckVariableHeader(connectReturnCode, false, properties);
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0X02);
        return new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
    }

    /**
     * 获取mqtt消息id
     *
     * @param mqttMessage mqtt消息
     * @return mqtt消息id
     */
    public static int getMessageId(MqttMessage mqttMessage) {
        Object object = mqttMessage.variableHeader();
        if (object instanceof MqttPublishVariableHeader) {
            return ((MqttPublishVariableHeader) object).packetId();
        } else if (object instanceof MqttMessageIdVariableHeader) {
            return ((MqttMessageIdVariableHeader) object).messageId();
        } else {
            // client send connect key
            return -1;
        }
    }

    /**
     * 获取mqtt publish消息payload bytes并重置其reader index
     *
     * @param mqttMessage mqtt消息
     * @return mqtt消息payload bytes
     */
    public static byte[] copyPublishPayload(MqttPublishMessage mqttMessage) {
        ByteBuf byteBuf = mqttMessage.payload();
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);
        byteBuf.resetReaderIndex();
        return bytes;
    }

    /**
     * 将{@link MqttPublishMessage}转换成{@link MqttMessageReplica}
     */
    public static MqttMessageReplica toReplica(String clientId, MqttPublishMessage message, long timestamp) {
        MqttPublishVariableHeader variableHeader = message.variableHeader();
        MqttFixedHeader fixedHeader = message.fixedHeader();
        return MqttMessageReplica.builder()
                .timestamp(timestamp)
                .clientId(clientId)
                .topic(variableHeader.topicName())
                .setRetain(fixedHeader.isRetain())
                .qos(fixedHeader.qosLevel().value())
                .properties(MqttMessageUtils.toStringProperties(variableHeader.properties()))
                .payload(MqttMessageUtils.copyPublishPayload(message))
                .build();
    }
}
