package org.kin.mqtt.broker.rule.impl;

import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.rule.BridgeAction;
import org.kin.mqtt.broker.rule.RuleChainContext;
import org.kin.mqtt.broker.rule.RuleNode;
import org.kin.mqtt.broker.rule.definition.KafkaActionDefinition;

/**
 * 将publish消息数据桥接到kafka
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see org.kin.mqtt.broker.bridge.BridgeType#KAFKA
 */
public class KafkaBridgeAction extends BridgeAction<KafkaActionDefinition> {
    public KafkaBridgeAction(KafkaActionDefinition definition) {
        super(definition);
    }

    public KafkaBridgeAction(KafkaActionDefinition definition, RuleNode next) {
        super(definition, next);
    }

    @Override
    protected void preTransmit(RuleChainContext context) {
        context.getAttrs().updateAttr(BridgeAttrNames.KAFKA_TOPIC, definition.getTopic());
    }

    @Override
    protected BridgeType type() {
        return BridgeType.KAFKA;
    }
}
