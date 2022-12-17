package org.kin.mqtt.broker.rule.action.bridge.definition;

import org.kin.mqtt.broker.rule.action.ActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.MqttTopicAction;

import java.util.Objects;

/**
 * topic转发规则定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see MqttTopicAction
 */
public final class MqttTopicActionDefinition implements ActionDefinition {
    /** 要转发的topic */
    private String topic;

    private MqttTopicActionDefinition() {
    }

    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final MqttTopicActionDefinition mqttTopicActionDefinition = new MqttTopicActionDefinition();

        public Builder topic(String topic) {
            mqttTopicActionDefinition.topic = topic;
            return this;
        }

        public MqttTopicActionDefinition build() {
            return mqttTopicActionDefinition;
        }
    }

    //setter && getter
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MqttTopicActionDefinition)) {
            return false;
        }
        MqttTopicActionDefinition that = (MqttTopicActionDefinition) o;
        return Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic);
    }

    @Override
    public String toString() {
        return "MqttTopicActionDefinition{" +
                super.toString() +
                "topic='" + topic + '\'' +
                "} ";
    }
}
