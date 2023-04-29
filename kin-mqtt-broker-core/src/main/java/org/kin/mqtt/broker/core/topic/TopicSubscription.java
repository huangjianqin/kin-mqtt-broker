package org.kin.mqtt.broker.core.topic;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.TopicNames;
import org.kin.mqtt.broker.core.MqttSession;

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
    private MqttSession mqttSession;
    /** 订阅qos */
    private MqttQoS qos;
    /** 共享topic用到, 用于共享topic区分组, 默认null */
    private String group;
    /** 如果mqtt client订阅了自己的publish的topic, 将noLocal设置为true, 则mqtt client不会收到该topic自己publish的消息 */
    private boolean noLocal;
    /** true, 则转发publish消息时必须保留retain flag */
    private boolean retainAsPublished;

    /**
     * 用于移除订阅
     */
    public static TopicSubscription forRemove(String topic, MqttSession mqttSession) {
        return new TopicSubscription(topic, mqttSession);
    }

    public TopicSubscription(MqttTopicSubscription rawSubscription, MqttSession mqttSession) {
        this(mqttSession,
                rawSubscription.topicName(), rawSubscription.qualityOfService(),
                rawSubscription.option().isNoLocal(), rawSubscription.option().isRetainAsPublished());
    }

    public TopicSubscription(TopicSubscriptionReplica replica, MqttSession mqttSession) {
        this(mqttSession, replica.getTopic(), MqttQoS.valueOf(replica.getQos()),
                replica.isNoLocal(), replica.isRetainAsPublished());
    }

    private TopicSubscription(MqttSession mqttSession,
                              String rawTopic,
                              MqttQoS qos,
                              boolean noLocal,
                              boolean retainAsPublished) {
        this.rawTopic = rawTopic;
        this.qos = qos;
        this.mqttSession = mqttSession;
        if (TopicNames.isShareTopic(rawTopic)) {
            //共享topic
            String[] splits = rawTopic.split(TopicFilter.SEPARATOR, 3);
            this.group = splits[1];
            this.topic = splits[2];
        } else {
            //普通topic
            this.topic = this.rawTopic;
        }
        this.noLocal = noLocal;
        this.retainAsPublished = retainAsPublished;
    }

    /**
     * 用于移除订阅
     */
    private TopicSubscription(String topic, MqttSession mqttSession) {
        this.rawTopic = topic;
        this.topic = topic;
        this.mqttSession = mqttSession;
    }

    /**
     * 用于内部topic subscription复制
     */
    private TopicSubscription(String rawTopic, String topic, MqttSession mqttSession, MqttQoS qos, String group) {
        this.rawTopic = rawTopic;
        this.topic = topic;
        this.mqttSession = mqttSession;
        this.qos = qos;
        this.group = group;
    }

    /**
     * publisher与broker的传输语义取决于发送的mqtt消息的qos
     * subscriber与broker的传输语义取决于接收到的mqtt消息的topic对应订阅时定义的qos
     * <p>
     * 当publish消息时, 需根据实际订阅qos和该消息qos来解决最后给该mqtt client publish的消息的qos.
     * 这里面取min, 保证了同时满足实际订阅qos和该消息qos
     *
     * @param mqttQoS dispatch的publish消息的qos
     * @return 结合publish消息后, 真实订阅信息
     */
    public TopicSubscription convert(MqttQoS mqttQoS) {
        return new TopicSubscription(rawTopic, topic, mqttSession,
                MqttQoS.valueOf(Math.min(mqttQoS.value(), qos.value())), group);
    }

    /**
     * 添加进{@link TopicFilter}才触发, 不然事实上并没有订阅成功
     */
    public void onLinked() {
        mqttSession.addSubscription(this);
    }

    /**
     * 取消订阅, 也是从{@link  TopicFilter}移除, 才触发, 不然事实上并没有取消成功
     */
    public void onUnlinked() {
        mqttSession.removeSubscription(this);
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
    public void setQos(MqttQoS qos) {
        this.qos = qos;
    }

    /**
     * 将{@link TopicSubscription}转换成{@link TopicSubscriptionReplica}
     */
    public TopicSubscriptionReplica toReplica() {
        TopicSubscriptionReplica replica = new TopicSubscriptionReplica();
        replica.setTopic(rawTopic);
        replica.setQos(qos.value());
        replica.setNoLocal(noLocal);
        replica.setRetainAsPublished(retainAsPublished);
        return replica;
    }

    //getter
    public String getRawTopic() {
        return rawTopic;
    }

    public String getTopic() {
        return topic;
    }

    public MqttQoS getQos() {
        return qos;
    }

    public MqttSession getMqttSession() {
        return mqttSession;
    }

    public String getGroup() {
        return group;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    public boolean isRetainAsPublished() {
        return retainAsPublished;
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
        return Objects.equals(topic, that.topic) && Objects.equals(mqttSession, that.mqttSession);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, mqttSession);
    }

    @Override
    public String toString() {
        return "TopicSubscription{" +
                "rawTopic='" + rawTopic + '\'' +
                ", topic='" + topic + '\'' +
                ", mqttSession=" + mqttSession +
                ", qos=" + qos +
                ", group='" + group + '\'' +
                ", noLocal='" + noLocal + '\'' +
                ", retainAsPublished='" + retainAsPublished + '\'' +
                '}';
    }
}
