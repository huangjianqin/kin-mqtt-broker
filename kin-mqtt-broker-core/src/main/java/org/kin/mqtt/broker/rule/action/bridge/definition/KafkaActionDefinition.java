package org.kin.mqtt.broker.rule.action.bridge.definition;

import org.kin.mqtt.broker.rule.action.bridge.KafkaBridgeAction;

/**
 * kafka bridge动作规则定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see KafkaBridgeAction
 */
public class KafkaActionDefinition extends BridgeActionDefinition {
    /** kafka topic */
    private String topic;

    private KafkaActionDefinition() {
    }

    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder extends BridgeActionDefinition.Builder<KafkaActionDefinition> {
        protected Builder() {
            super(new KafkaActionDefinition());
        }

        public Builder topic(String topic) {
            definition.topic = topic;
            return this;
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
    public String toString() {
        return "KafkaActionDefinition{" +
                super.toString() +
                "topic='" + topic + '\'' +
                "} ";
    }
}
