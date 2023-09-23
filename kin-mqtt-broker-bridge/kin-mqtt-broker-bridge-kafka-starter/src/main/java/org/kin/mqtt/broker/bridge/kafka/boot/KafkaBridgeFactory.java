package org.kin.mqtt.broker.bridge.kafka.boot;

import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.bridge.BridgeConfiguration;
import org.kin.mqtt.broker.bridge.BridgeFactory;

/**
 * {@link KafkaBridge} factory实现
 *
 * @author huangjianqin
 * @date 2023/5/26
 */
@Extension("kafka")
public class KafkaBridgeFactory implements BridgeFactory<KafkaBridge> {
    @Override
    public KafkaBridge create(BridgeConfiguration config) {
        return new KafkaBridge(config);
    }
}
