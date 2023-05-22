package org.kin.mqtt.broker.rule.action.bridge;

import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.rule.RuleContext;
import org.kin.mqtt.broker.rule.action.bridge.definition.KafkaBridgeActionDefinition;

/**
 * 将publish消息数据桥接到kafka
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see org.kin.mqtt.broker.bridge.BridgeType#KAFKA
 */
public class KafkaBridgeAction extends BridgeAction<KafkaBridgeActionDefinition> {
    public KafkaBridgeAction(KafkaBridgeActionDefinition definition) {
        super(definition);
    }

    @Override
    protected void preTransmit(RuleContext context) {
        context.getAttrs().updateAttr(BridgeAttrNames.KAFKA_TOPIC, definition.getTopic());
    }

    @Override
    protected BridgeType type() {
        return BridgeType.KAFKA;
    }
}
