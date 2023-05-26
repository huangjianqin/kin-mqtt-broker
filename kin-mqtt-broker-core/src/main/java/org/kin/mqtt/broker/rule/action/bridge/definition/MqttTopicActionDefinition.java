package org.kin.mqtt.broker.rule.action.bridge.definition;

import com.google.common.base.Preconditions;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.core.Type;
import org.kin.mqtt.broker.rule.action.ActionDefinition;
import org.kin.mqtt.broker.rule.action.ActionType;
import org.kin.mqtt.broker.rule.action.bridge.MqttTopicAction;

import java.util.Objects;

/**
 * topic转发规则定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see MqttTopicAction
 */
@Type(ActionType.MQTT_TOPIC)
public class MqttTopicActionDefinition implements ActionDefinition {
    private static final long serialVersionUID = 4014461797177714118L;
    /** 要转发的topic */
    private String topic;
    /** qos */
    private String qos;

    private MqttTopicActionDefinition() {
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void check() {
        Preconditions.checkArgument(StringUtils.isNotBlank(topic), "mqtt topic must be not blank");
        MqttQoS.valueOf(qos);
    }

    /** builder **/
    public static class Builder {
        private final MqttTopicActionDefinition mqttTopicActionDefinition = new MqttTopicActionDefinition();

        public Builder topic(String topic) {
            mqttTopicActionDefinition.topic = topic;
            return this;
        }

        public Builder qos(String qos) {
            mqttTopicActionDefinition.qos = qos;
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

    public String getQos() {
        return qos;
    }

    public void setQos(String qos) {
        this.qos = qos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttTopicActionDefinition that = (MqttTopicActionDefinition) o;
        return Objects.equals(topic, that.topic) && Objects.equals(qos, that.qos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, qos);
    }

    @Override
    public String toString() {
        return "MqttTopicActionDefinition{" +
                super.toString() +
                "topic='" + topic + '\'' +
                "qos='" + qos + '\'' +
                "} ";
    }
}
