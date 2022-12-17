package org.kin.mqtt.broker.rule.action.bridge.definition;

import org.kin.mqtt.broker.rule.action.bridge.RabbitMQBridgeAction;

/**
 * rabbitmq bridge动作规则定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see RabbitMQBridgeAction
 */
public final class RabbitMQActionDefinition extends BridgeActionDefinition {
    /** rabbitmq queue */
    private String queue;

    private RabbitMQActionDefinition() {
    }

    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder extends BridgeActionDefinition.Builder<RabbitMQActionDefinition> {
        protected Builder() {
            super(new RabbitMQActionDefinition());
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
    public String toString() {
        return "RabbitMQActionDefinition{" +
                super.toString() +
                "queue='" + queue + '\'' +
                "} ";
    }
}
