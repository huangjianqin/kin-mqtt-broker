package org.kin.mqtt.broker.core.message;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 对{@link MqttMessage}简单包装, 新增一些额外的信息
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class MqttMessageContext<T extends MqttMessage> {
    /** 真正mqtt消息 */
    private final T message;
    /** 接受或创建mqtt消息的时间戳(毫秒) */
    private final long recTime;
    /** 是否来自于集群 */
    private final boolean fromCluster;
    /** 过期时间(毫秒) */
    private final long expireTime;
    /** 来自于哪个broker */
    private final String brokerId;
    /** 发送该mqtt消息的client id */
    private final String clientId;

    private MqttMessageContext(T message, long recTime, boolean fromCluster, long expireTime, String brokerId, String clientId) {
        this.message = message;
        this.recTime = recTime;
        this.fromCluster = fromCluster;
        this.expireTime = expireTime;
        this.brokerId = brokerId;
        this.clientId = clientId;
    }

    @SuppressWarnings("unchecked")
    private MqttMessageContext(T message, boolean fromCluster, String brokerId, String clientId) {
        this.message = message;
        this.recTime = System.currentTimeMillis();
        this.fromCluster = fromCluster;
        this.brokerId = brokerId;
        this.clientId = clientId;

        if (message instanceof MqttPublishMessage) {
            MqttPublishMessage pubMessage = (MqttPublishMessage) message;
            MqttPublishVariableHeader variableHeader = pubMessage.variableHeader();
            MqttProperties mqttProperties = variableHeader.properties();
            MqttProperties.MqttProperty<Integer> pubExpiryIntervalProp = mqttProperties.getProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value());
            if (Objects.nonNull(pubExpiryIntervalProp)) {
                //已过期
                expireTime = recTime + TimeUnit.SECONDS.toMillis(pubExpiryIntervalProp.value());
            } else {
                expireTime = -1;
            }
        } else {
            expireTime = -1;
        }
    }

    public static <T extends MqttMessage> MqttMessageContext<T> common(T message, String brokerId, String clientId) {
        return new MqttMessageContext<>(message, false, brokerId, clientId);
    }

    /**
     * 从{@code messageContext}复制字段值并替换其包装的消息
     */
    @SuppressWarnings("unchecked")
    public static <T extends MqttMessage> MqttMessageContext<T> common(MqttMessageContext<T> messageContext, MqttMessage message) {
        return new MqttMessageContext<>((T) message, messageContext.recTime,
                messageContext.fromCluster, messageContext.expireTime,
                messageContext.brokerId, messageContext.clientId);
    }

    /**
     * 将集群广播的mqtt message包装成{@link MqttMessageContext}实例
     */
    public static MqttMessageContext<MqttPublishMessage> fromCluster(MqttMessageReplica replica) {
        return fromReplica(replica, true);
    }

    /**
     * {@link MqttMessageReplica}实例转换成{@link MqttMessageContext}实例
     *
     * @param fromCluster 用于区分本broker还是remote broker的{@link MqttMessageReplica}实例
     */
    public static MqttMessageContext<MqttPublishMessage> fromReplica(MqttMessageReplica replica, boolean fromCluster) {
        return new MqttMessageContext<>(MqttMessageHelper.createPublish(replica), fromCluster,
                replica.getBrokerId(), replica.getClientId());
    }

    /**
     * 将{@link MqttMessageContext<MqttPublishMessage>}转换成{@link MqttMessageReplica}
     */
    @SuppressWarnings("unchecked")
    public MqttMessageReplica toReplica() {
        return MqttMessageHelper.toReplica((MqttMessageContext<MqttPublishMessage>) this);
    }

    /**
     * 消息是否过期, 仅仅针对publish消息
     */
    public boolean isExpire() {
        return expireTime > 0 && System.currentTimeMillis() >= expireTime;
    }

    //getter
    public T getMessage() {
        return message;
    }

    public long getRecTime() {
        return recTime;
    }

    public boolean isFromCluster() {
        return fromCluster;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public String getClientId() {
        return clientId;
    }
}
