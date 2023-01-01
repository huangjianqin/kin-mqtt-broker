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
public class MqttMessageWrapper<T extends MqttMessage> {
    /** 真正mqtt消息 */
    private T message;
    /** 接受或创建mqtt消息的时间戳ms */
    private final long timestamp;
    /** 是否来自于集群 */
    private final boolean fromCluster;
    /** 过期时间 */
    private final long expireTimeMs;

    @SuppressWarnings("unchecked")
    public MqttMessageWrapper(T message, boolean fromCluster) {
        this.message = message;
        this.timestamp = System.currentTimeMillis();
        this.fromCluster = fromCluster;

        if (message instanceof MqttPublishMessage) {
            MqttPublishMessage pubMessage = (MqttPublishMessage) message;
            MqttPublishVariableHeader variableHeader = pubMessage.variableHeader();
            MqttProperties mqttProperties = variableHeader.properties();
            MqttProperties.MqttProperty<Integer> pubExpiryIntervalProp = mqttProperties.getProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value());
            if (Objects.nonNull(pubExpiryIntervalProp)) {
                //已过期
                expireTimeMs = timestamp + TimeUnit.SECONDS.toMillis(pubExpiryIntervalProp.value());
            } else {
                expireTimeMs = 0;
            }
        } else {
            expireTimeMs = 0;
        }
    }

    /**
     * 从{@code wrapper}复制字段值并替换其包装的消息
     */
    public MqttMessageWrapper(MqttMessageWrapper<T> wrapper, T message) {
        this.message = message;
        this.timestamp = wrapper.timestamp;
        this.fromCluster = wrapper.fromCluster;
        this.expireTimeMs = wrapper.expireTimeMs;
    }

    public static <T extends MqttMessage> MqttMessageWrapper<T> common(T message) {
        return new MqttMessageWrapper<>(message, false);
    }

    public static MqttMessageWrapper<MqttPublishMessage> fromCluster(MqttMessageReplica replica) {
        return new MqttMessageWrapper<>(MqttMessageUtils.createPublish(replica), true);
    }

    /**
     * 替换绑定mqtt消息
     *
     * @param message mqtt消息
     * @return this
     */
    @SuppressWarnings("unchecked")
    public MqttMessageWrapper<T> replaceMessage(MqttMessage message) {
        this.message = (T) message;
        return this;
    }

    /**
     * 消息是否过期, 仅仅针对publish消息
     */
    public boolean isExpire() {
        return expireTimeMs > 0 && System.currentTimeMillis() >= expireTimeMs;
    }

    //getter
    public T getMessage() {
        return message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isFromCluster() {
        return fromCluster;
    }

    public long getExpireTimeMs() {
        return expireTimeMs;
    }
}
