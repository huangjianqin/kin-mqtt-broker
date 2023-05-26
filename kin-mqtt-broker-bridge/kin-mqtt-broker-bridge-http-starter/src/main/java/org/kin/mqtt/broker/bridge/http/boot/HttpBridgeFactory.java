package org.kin.mqtt.broker.bridge.http.boot;

import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.bridge.BridgeFactory;
import org.kin.mqtt.broker.bridge.definition.HttpBridgeDefinition;

/**
 * {@link HttpBridge} factory实现
 * @author huangjianqin
 * @date 2023/5/26
 */
@Extension("http")
public final class HttpBridgeFactory implements BridgeFactory<HttpBridgeDefinition, HttpBridge> {
    @Override
    public HttpBridge create(HttpBridgeDefinition httpBridgeDefinition) {
        return new HttpBridge(httpBridgeDefinition);
    }
}
