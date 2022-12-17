package org.kin.mqtt.broker.rule.action.bridge;

import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.rule.RuleContext;
import org.kin.mqtt.broker.rule.action.bridge.definition.RabbitMQActionDefinition;

/**
 * 将publish消息数据桥接到rabbitmq
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see org.kin.mqtt.broker.bridge.BridgeType#RABBITMQ
 */
public final class RabbitMQBridgeAction extends BridgeAction<RabbitMQActionDefinition> {
    public RabbitMQBridgeAction(RabbitMQActionDefinition definition) {
        super(definition);
    }

    @Override
    protected void preStart(RuleContext context) {
        context.getAttrs().updateAttr(BridgeAttrNames.RABBITMQ_QUEUE, definition.getQueue());
    }

    @Override
    protected BridgeType type() {
        return BridgeType.RABBITMQ;
    }
}