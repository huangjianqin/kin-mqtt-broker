package org.kin.mqtt.broker.core.topic;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.mqtt.broker.core.MqttChannel;

import java.util.Objects;

/**
 * topic订阅信息
 *
 * @author huangjianqin
 * @date 2022/11/13
 */
public class TopicSubscription {
    /** 订阅的topic name */
    private final String topic;
    /** 发起订阅的mqtt连接 */
    private MqttChannel mqttChannel;
    /** 订阅qos */
    private final MqttQoS qoS;

    public static TopicSubscription forRemove(String topic, MqttChannel mqttChannel) {
        return new TopicSubscription(topic, null, mqttChannel);
    }

    public TopicSubscription(String topic, MqttQoS qoS, MqttChannel mqttChannel) {
        this.topic = topic;
        this.qoS = qoS;
        this.mqttChannel = mqttChannel;
    }

    /**
     * 当publish消息时, 需根据实际订阅qos和该消息qos来解决最后给该mqtt client publish的消息的qos.
     * 这里面取min, 保证了同时满足实际订阅qos和该消息qos
     *
     * @param mqttQoS dispatch的publish消息的qos
     * @return 结合publish消息后, 真实订阅信息
     */
    public TopicSubscription convert(MqttQoS mqttQoS) {
        MqttQoS minQos = MqttQoS.valueOf(Math.min(mqttQoS.value(), qoS.value()));
        return new TopicSubscription(topic, minQos, mqttChannel);
    }

    /**
     * 添加进{@link TopicFilter}才触发, 不然事实上并没有订阅成功
     */
    public void onLinked() {
        mqttChannel.addSubscription(this);
    }

    /**
     * 取消订阅, 也是从{@link  TopicFilter}移除, 才触发, 不然事实上并没有取消成功
     */
    public void onUnlinked() {
        mqttChannel.removeSubscription(this);
    }

    /**
     * 重新绑定mqtt channel
     */
    public void onRelink(MqttChannel mqttChannel) {
        this.mqttChannel = mqttChannel;
    }

    //getter
    public String getTopic() {
        return topic;
    }

    public MqttQoS getQoS() {
        return qoS;
    }

    public MqttChannel getMqttChannel() {
        return mqttChannel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopicSubscription)) {
            return false;
        }
        TopicSubscription that = (TopicSubscription) o;
        return Objects.equals(topic, that.topic) && Objects.equals(mqttChannel, that.mqttChannel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, mqttChannel);
    }

    @Override
    public String toString() {
        return "TopicSubscription{" +
                "topic='" + topic + '\'' +
                ", qoS=" + qoS +
                ", mqttChannel=" + mqttChannel +
                '}';
    }
}
