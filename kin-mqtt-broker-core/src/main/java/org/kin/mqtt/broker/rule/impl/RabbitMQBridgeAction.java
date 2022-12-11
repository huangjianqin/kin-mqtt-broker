package org.kin.mqtt.broker.rule.impl;

import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.rule.BridgeAction;
import org.kin.mqtt.broker.rule.RuleChainContext;
import org.kin.mqtt.broker.rule.RuleNode;
import org.kin.mqtt.broker.rule.definition.RabbitMQActionDefinition;

/**
 * 将publish消息数据桥接到rabbitmq
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see org.kin.mqtt.broker.bridge.BridgeType#RABBITMQ
 */
public class RabbitMQBridgeAction extends BridgeAction<RabbitMQActionDefinition> {
    public RabbitMQBridgeAction(RabbitMQActionDefinition definition) {
        super(definition);
    }

    public RabbitMQBridgeAction(RabbitMQActionDefinition definition, RuleNode next) {
        super(definition, next);
    }

    @Override
    protected void preTransmit(RuleChainContext context) {
        context.getAttrs().updateAttr(BridgeAttrNames.RABBITMQ_QUEUE, definition.getQueue());
    }

    @Override
    protected BridgeType type() {
        return BridgeType.RABBITMQ;
    }
}