package org.kin.mqtt.broker.rule.impl;

import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.rule.BridgeAction;
import org.kin.mqtt.broker.rule.RuleChainContext;
import org.kin.mqtt.broker.rule.RuleNode;
import org.kin.mqtt.broker.rule.definition.HttpActionDefinition;

/**
 * 将publish消息数据桥接到http接口
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see org.kin.mqtt.broker.bridge.BridgeType#HTTP
 */
public class HttpBridgeAction extends BridgeAction<HttpActionDefinition> {
    public HttpBridgeAction(HttpActionDefinition definition) {
        super(definition);
    }

    public HttpBridgeAction(HttpActionDefinition definition, RuleNode next) {
        super(definition, next);
    }

    @Override
    protected void preTransmit(RuleChainContext context) {
        context.getAttrs().updateAttr(BridgeAttrNames.HTTP_URI, definition.getUri());
        context.getAttrs().updateAttr(BridgeAttrNames.HTTP_HEADERS, definition.getHeaders());
    }

    @Override
    protected BridgeType type() {
        return BridgeType.HTTP;
    }
}