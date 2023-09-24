package org.kin.mqtt.broker.rule.action.impl.bridge;

import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.rule.RuleContext;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;

/**
 * 将publish消息数据桥接到kafka
 *
 * @author huangjianqin
 * @date 2022/12/11
 */
public class KafkaBridgeAction extends BridgeAction {
    public KafkaBridgeAction(ActionConfiguration config) {
        super(config);
    }

    @Override
    protected void preTransmit(RuleContext context) {
        context.getAttrs().updateAttr(BridgeAttrNames.KAFKA_TOPIC, config.get(BridgeActionConstants.KAFKA_TOPIC_KEY));
    }
}
