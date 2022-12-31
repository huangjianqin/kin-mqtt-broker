package org.kin.mqtt.broker.core.topic;

import io.netty.handler.codec.mqtt.MqttQoS;
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
    private final MqttQoS qoS;
    /** 共享topic用到, 用于共享topic区分组, 默认null */
    private String group;

    public TopicSubscription(String topic, MqttChannel mqttChannel) {
        this(topic, null, mqttChannel);
    }

    public TopicSubscription(String topic, MqttQoS qoS, MqttChannel mqttChannel) {
        this.rawTopic = topic;
        this.qoS = qoS;
        this.mqttChannel = mqttChannel;
        if (TopicNames.isShareTopic(rawTopic)) {
            //共享topic
            String[] splits = topic.split(TopicFilter.SEPARATOR, 3);
            this.group = splits[1];
            this.topic = splits[2];
        } else {
            //普通topic
            this.topic = this.rawTopic;
        }
    }

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
                '}';
    }
}
