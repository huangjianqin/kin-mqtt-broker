package org.kin.mqtt.broker.bridge.rabbitmq;

import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.bridge.BridgeConfiguration;
import org.kin.mqtt.broker.bridge.BridgeFactory;

/**
 * {@link RabbitMQBridge} factory实现
 *
 * @author huangjianqin
 * @date 2023/5/26
 */
@Extension("rabbitMQ")
public class RabbitMQBridgeFactory implements BridgeFactory<RabbitMQBridge> {
    @Override
    public RabbitMQBridge create(BridgeConfiguration config) {
        return new RabbitMQBridge(config);
    }
}
