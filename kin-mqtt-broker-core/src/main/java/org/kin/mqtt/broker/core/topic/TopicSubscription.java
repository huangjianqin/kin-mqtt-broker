package org.kin.mqtt.broker.core.topic;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.TopicNames;
import org.kin.mqtt.broker.core.MqttChannel;

import java.util.Objects;

/**
 * topic订阅信息
 *
 * @author huangjianqin
 * @date 2022/11/13
 */
public class TopicSubscription {
    /** 原始订阅的topic name, 即mqtt client发过的订阅topic name */
    private final String rawTopic;
    /** 订阅的topic name */
    private final String topic;
    /** 发起订阅的mqtt连接 */
    private MqttChannel mqttChannel;
    /** 订阅qos */
    private MqttQoS qoS;
    /** 共享topic用到, 用于共享topic区分组, 默认null */
    private String group;
    /** 如果mqtt client订阅了自己的publish的topic, 将noLocal设置为true, 则mqtt client不会收到该topic自己publish的消息 */
    private boolean noLocal;

    /**
     * 用于移除订阅
     */
    public static TopicSubscription forRemove(String topic, MqttChannel mqttChannel) {
        return new TopicSubscription(topic, mqttChannel);
    }

    public TopicSubscription(MqttTopicSubscription rawSubscription, MqttChannel mqttChannel) {
        this.rawTopic = rawSubscription.topicName();
        this.qoS = rawSubscription.qualityOfService();
        this.mqttChannel = mqttChannel;
        if (TopicNames.isShareTopic(rawTopic)) {
            //共享topic
            String[] splits = rawTopic.split(TopicFilter.SEPARATOR, 3);
            this.group = splits[1];
            this.topic = splits[2];
        } else {
            //普通topic
            this.topic = this.rawTopic;
        }
        MqttSubscriptionOption option = rawSubscription.option();
        this.noLocal = option.isNoLocal();
    }

    /**
     * 用于移除订阅
     */
    private TopicSubscription(String topic, MqttChannel mqttChannel) {
        this.rawTopic = topic;
        this.topic = topic;
        this.mqttChannel = mqttChannel;
    }

    /**
     * 用于内部topic subscription复制
     */
    private TopicSubscription(String rawTopic, String topic, MqttChannel mqttChannel, MqttQoS qoS, String group) {
        this.rawTopic = rawTopic;
        this.topic = topic;
        this.mqttChannel = mqttChannel;
        this.qoS = qoS;
        this.group = group;
    }

    /**
     * 当publish消息时, 需根据实际订阅qos和该消息qos来解决最后给该mqtt client publish的消息的qos.
     * 这里面取min, 保证了同时满足实际订阅qos和该消息qos
     *
     * @param mqttQoS dispatch的publish消息的qos
     * @return 结合publish消息后, 真实订阅信息
     */
    public TopicSubscription convert(MqttQoS mqttQoS) {
        return new TopicSubscription(rawTopic, topic, mqttChannel,
                MqttQoS.valueOf(Math.min(mqttQoS.value(), qoS.value())), group);
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
     * 判断是否是共享订阅
     */
    public boolean isShare() {
        return StringUtils.isNotBlank(group);
    }

    /**
     * 更新qos
     */
    public void setQoS(MqttQoS qoS) {
        this.qoS = qoS;
    }

    //getter
    public String getRawTopic() {
        return rawTopic;
    }

    public String getTopic() {
        return topic;
    }

    public MqttQoS getQoS() {
        return qoS;
    }

    public MqttChannel getMqttChannel() {
        return mqttChannel;
    }

    public String getGroup() {
        return group;
    }

    public boolean isNoLocal() {
        return noLocal;
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
                "rawTopic='" + rawTopic + '\'' +
                ", topic='" + topic + '\'' +
                ", mqttChannel=" + mqttChannel +
                ", qoS=" + qoS +
                ", group='" + group + '\'' +
                ", noLocal='" + noLocal + '\'' +
                '}';
    }
}
