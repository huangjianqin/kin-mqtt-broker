package org.kin.mqtt.broker.bridge.http.boot;

import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.bridge.BridgeConfiguration;
import org.kin.mqtt.broker.bridge.BridgeFactory;

/**
 * {@link HttpBridge} factory实现
 *
 * @author huangjianqin
 * @date 2023/5/26
 */
@Extension("http")
public final class HttpBridgeFactory implements BridgeFactory<HttpBridge> {
    @Override
    public HttpBridge create(BridgeConfiguration config) {
        return new HttpBridge(config);
    }
}
