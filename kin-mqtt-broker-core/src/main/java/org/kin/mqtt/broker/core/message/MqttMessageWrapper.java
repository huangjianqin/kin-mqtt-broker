package org.kin.mqtt.broker.core.message;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * 对{@link MqttMessage}简单包装, 新增一些额外的信息
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public final class MqttMessageWrapper<T extends MqttMessage> {
    /** 真正mqtt消息 */
    private final T message;
    /** 接受或创建mqtt消息的时间戳ms */
    private final long timestamp = System.currentTimeMillis();
    /** 是否来自于集群 */
    private final boolean fromCluster;

    public MqttMessageWrapper(T message, boolean fromCluster) {
        this.message = message;
        this.fromCluster = fromCluster;
    }

    public static <T extends MqttMessage> MqttMessageWrapper<T> common(T message) {
        return new MqttMessageWrapper<>(message, false);
    }

    public static MqttMessageWrapper<MqttPublishMessage> fromCluster(MqttMessageReplica replica) {
        MqttPublishMessage publishMessage = MqttMessageUtils.createPublish(false,
                MqttQoS.valueOf(replica.getQos()),
                0,
                replica.getTopic(),
                PooledByteBufAllocator.DEFAULT.buffer().writeBytes(replica.getMessage()),
                replica.getProperties());
        return new MqttMessageWrapper<>(publishMessage, true);
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
}
