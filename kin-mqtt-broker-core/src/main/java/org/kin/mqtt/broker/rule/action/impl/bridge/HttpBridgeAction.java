package org.kin.mqtt.broker.rule.action.impl.bridge;

import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.rule.RuleContext;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;

import java.util.Collections;

/**
 * 将publish消息数据桥接到http接口
 *
 * @author huangjianqin
 * @date 2022/12/11
 */
public class HttpBridgeAction extends BridgeAction {
    public HttpBridgeAction(ActionConfiguration config) {
        super(config);
    }

    @Override
    protected void preTransmit(RuleContext context) {
        context.getAttrs().updateAttr(BridgeAttrNames.HTTP_URI, config.get(BridgeActionConstants.HTTP_URI_KEY));
        context.getAttrs().updateAttr(BridgeAttrNames.HTTP_HEADERS, config.get(BridgeActionConstants.HTTP_HEADERS_KEY, Collections.emptyMap()));
    }
}