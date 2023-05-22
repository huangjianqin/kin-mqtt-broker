package org.kin.mqtt.broker.rule.action.bridge.definition;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.rule.action.bridge.RabbitMQBridgeAction;

import java.util.Objects;

/**
 * rabbitmq bridge动作规则定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see RabbitMQBridgeAction
 */
public class RabbitMQBridgeActionDefinition extends BridgeActionDefinition {
    /** rabbitmq queue */
    private String queue;

    private RabbitMQBridgeActionDefinition() {
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void check() {
        Preconditions.checkArgument(StringUtils.isNotBlank(queue), "rabbitMQ queue must be not blank");
    }

    /** builder **/
    public static class Builder extends BridgeActionDefinition.Builder<RabbitMQBridgeActionDefinition, Builder> {
        protected Builder() {
            super(new RabbitMQBridgeActionDefinition());
        }

        public Builder queue(String queue) {
            definition.queue = queue;
            return this;
        }
    }

    //setter && getter
    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RabbitMQBridgeActionDefinition that = (RabbitMQBridgeActionDefinition) o;
        return Objects.equals(queue, that.queue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queue);
    }

    @Override
    public String toString() {
        return "RabbitMQActionDefinition{" +
                super.toString() +
                "queue='" + queue + '\'' +
                "} ";
    }
}
