package org.kin.mqtt.broker.bridge.rabbitmq;

import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.bridge.BridgeFactory;
import org.kin.mqtt.broker.bridge.definition.RabbitMQBridgeDefinition;

/**
 * {@link RabbitMQBridge} factory实现
 * @author huangjianqin
 * @date 2023/5/26
 */
@Extension("rabbitMQ")
public class RabbitMQBridgeFactory implements BridgeFactory<RabbitMQBridgeDefinition, RabbitMQBridge> {
    @Override
    public RabbitMQBridge create(RabbitMQBridgeDefinition definition) {
        return new RabbitMQBridge(definition);
    }
}
