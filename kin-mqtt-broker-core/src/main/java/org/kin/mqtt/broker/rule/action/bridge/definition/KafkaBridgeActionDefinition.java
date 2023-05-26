package org.kin.mqtt.broker.rule.action.bridge.definition;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.core.Type;
import org.kin.mqtt.broker.rule.action.ActionType;
import org.kin.mqtt.broker.rule.action.bridge.KafkaBridgeAction;

import java.util.Objects;

/**
 * kafka bridge动作规则定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see KafkaBridgeAction
 */
@Type(ActionType.KAFKA_BRIDGE)
public class KafkaBridgeActionDefinition extends BridgeActionDefinition {
    /** kafka topic */
    private String topic;

    private KafkaBridgeActionDefinition() {
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void check() {
        super.check();
        Preconditions.checkArgument(StringUtils.isNotBlank(topic), "kafka topic must be not blank");
    }

    /** builder **/
    public static class Builder extends BridgeActionDefinition.Builder<KafkaBridgeActionDefinition, Builder> {
        protected Builder() {
            super(new KafkaBridgeActionDefinition());
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        KafkaBridgeActionDefinition that = (KafkaBridgeActionDefinition) o;
        return Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), topic);
    }

    @Override
    public String toString() {
        return "KafkaActionDefinition{" +
                super.toString() +
                "topic='" + topic + '\'' +
                "} ";
    }
}
