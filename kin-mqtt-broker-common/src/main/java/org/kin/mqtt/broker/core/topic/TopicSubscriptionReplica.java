package org.kin.mqtt.broker.core.topic;

import java.io.Serializable;
import java.util.Objects;

/**
 * mqtt topic订阅持久化数据
 *
 * @author huangjianqin
 * @date 2023/4/19
 */
public class TopicSubscriptionReplica implements Serializable {
    private static final long serialVersionUID = -3415159423404617484L;

    /** 订阅的原始topic name */
    private String topic;
    /** 订阅qos */
    private int qos;
    /** 如果mqtt client订阅了自己的publish的topic, 将noLocal设置为true, 则mqtt client不会收到该topic自己publish的消息 */
    private boolean noLocal;
    /** true, 则转发publish消息时必须保留retain flag */
    private boolean retainAsPublished;

    //setter && getter
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    public void setNoLocal(boolean noLocal) {
        this.noLocal = noLocal;
    }

    public boolean isRetainAsPublished() {
        return retainAsPublished;
    }

    public void setRetainAsPublished(boolean retainAsPublished) {
        this.retainAsPublished = retainAsPublished;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicSubscriptionReplica)) return false;
        TopicSubscriptionReplica that = (TopicSubscriptionReplica) o;
        return qos == that.qos && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, qos);
    }
}
