package org.kin.mqtt.broker.core.retry;

import io.netty.handler.codec.mqtt.MqttMessageType;
import org.kin.framework.Closeable;
import org.kin.mqtt.broker.core.MqttSession;

/**
 * 针对已发送的qos=012的消息进行重试管理
 *
 * @author huangjianqin
 * @date 2022/11/13
 */
public interface RetryService extends Closeable {
    /** mqtt消息发送retry id-channel占位 */
    long MQTT_MESSAGE_RETRY_ID_CHANNEL_MASK = 0xFFFFFFFF00000000L;
    /** mqtt消息发送retry id-channel位移n位 */
    int MQTT_MESSAGE_RETRY_ID_CHANNEL_SHIFT = 32;
    /** mqtt消息发送retry id-channel占位 */
    long MQTT_MESSAGE_RETRY_ID_TYPE_MASK = 0x00000000F0000000L;
    /** mqtt消息发送retry id-mqtt消息类型位移n位 */
    int MQTT_MESSAGE_RETRY_ID_TYPE_SHIFT = 28;
    /** mqtt消息发送retry id-mqtt消息占位 */
    long MQTT_MESSAGE_RETRY_ID_MESSAGE_ID_MASK = 0x000000000FFFFFFFL;

    /**
     * 生成唯一ID, 用于retry或者其他用途
     *
     * @param mqttSession mqtt session
     * @param type        mqtt消息类型
     * @param messageId   mqtt消息package id
     * @return 唯一ID, 即32位connection hashcode + 4位mqtt消息类型 + 28位mqtt消息package id
     */
    static long genMqttMessageRetryId(MqttSession mqttSession, MqttMessageType type, int messageId) {
        return genMqttMessageRetryId(mqttSession.getChannelHashCode(), type.value(), messageId);
    }

    /**
     * 生成唯一ID, 用于retry或者其他用途
     *
     * @param channelHashCode mqtt session channel hashcode
     * @param type            mqtt消息类型
     * @param messageId       mqtt消息package id
     * @return 唯一ID, 即32位connection hashcode + 4位mqtt消息类型 + 28位mqtt消息package id
     */
    static long genMqttMessageRetryId(int channelHashCode, int type, int messageId) {
        return (long) channelHashCode << 32 | (long) type << 28 | messageId;
    }

    /**
     * 解析mqtt消息发送retry id
     *
     * @param id mqtt消息发送retry id
     * @return retry id组成信息
     */
    static long[] parseMqttMessageRetryId(long id) {
        return new long[]{
                (id & MQTT_MESSAGE_RETRY_ID_CHANNEL_MASK) >>> MQTT_MESSAGE_RETRY_ID_CHANNEL_SHIFT,
                (id & MQTT_MESSAGE_RETRY_ID_TYPE_MASK) >>> MQTT_MESSAGE_RETRY_ID_TYPE_SHIFT,
                id & MQTT_MESSAGE_RETRY_ID_MESSAGE_ID_MASK
        };
    }

    /**
     * 执行retry task
     *
     * @param retry retry task
     */
    void execRetry(Retry retry);

    /**
     * 根据id获取retry task
     *
     * @param id retry id
     * @return retry task
     */
    Retry getRetry(long id);

    /**
     * 移除retry
     *
     * @param id retry id
     */
    void removeRetry(long id);
}
