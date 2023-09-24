package org.kin.mqtt.broker.rule.action.impl.bridge;

import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.rule.RuleContext;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;

/**
 * 将publish消息数据桥接到rabbitmq
 *
 * @author huangjianqin
 * @date 2022/12/11
 */
public class RabbitMQBridgeAction extends BridgeAction {
    public RabbitMQBridgeAction(ActionConfiguration config) {
        super(config);
    }

    @Override
    protected void preTransmit(RuleContext context) {
        context.getAttrs().updateAttr(BridgeAttrNames.RABBITMQ_QUEUE, config.get(BridgeActionConstants.RABBITMQ_QUEUE_KEY));
    }
}