package org.kin.mqtt.broker.bridge.kafka;

import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.bridge.BridgeFactory;
import org.kin.mqtt.broker.bridge.definition.KafkaBridgeDefinition;

/**
 * {@link KafkaBridge} factory实现
 * @author huangjianqin
 * @date 2023/5/26
 */
@Extension("kafka")
public class KafkaBridgeFactory implements BridgeFactory<KafkaBridgeDefinition, KafkaBridge> {
    @Override
    public KafkaBridge create(KafkaBridgeDefinition definition) {
        return new KafkaBridge(definition);
    }
}
