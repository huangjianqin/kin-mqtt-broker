package org.kin.mqtt.broker.rule.action.bridge;

import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.rule.RuleContext;
import org.kin.mqtt.broker.rule.action.bridge.definition.HttpActionDefinition;

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

    @Override
    protected void preTransmit(RuleContext context) {
        context.getAttrs().updateAttr(BridgeAttrNames.HTTP_URI, definition.getUri());
        context.getAttrs().updateAttr(BridgeAttrNames.HTTP_HEADERS, definition.getHeaders());
    }

    @Override
    protected BridgeType type() {
        return BridgeType.HTTP;
    }
}