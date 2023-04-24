package org.kin.mqtt.broker.cluster.event;

import org.kin.framework.reactor.event.ReactorEventBus;
import org.kin.mqtt.broker.cluster.BrokerManager;
import org.kin.mqtt.broker.cluster.MqttBrokerNode;
import org.kin.mqtt.broker.core.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * {@link AbstractMqttClusterEvent} consumer
 *
 * @author huangjianqin
 * @date 2023/4/24
 */
public abstract class AbstractMqttClusterEventConsumer<E extends AbstractMqttClusterEvent> implements MqttClusterEventConsumer<E> {
    private static final Logger log = LoggerFactory.getLogger(AbstractMqttClusterEventConsumer.class);

    /** 集群节点管理 */
    protected final BrokerManager brokerManager;

    protected AbstractMqttClusterEventConsumer(BrokerManager brokerManager) {
        this.brokerManager = brokerManager;
    }

    @Override
    public final void consume(ReactorEventBus eventBus, E event) {
        String address = event.getAddress();
        MqttBrokerNode node = brokerManager.getNode(address);
        if (Objects.isNull(node)) {
            throw new MqttException(String.format("receive cluster event from node '%s' which is not in cluster", address));
        }
        consume(eventBus, node, event);
    }

    /**
     * 事件消费逻辑实现
     *
     * @param eventBus 所属{@link ReactorEventBus}
     * @param node     发送event broker节点
     * @param event    事件
     */
    protected abstract void consume(ReactorEventBus eventBus, @Nullable MqttBrokerNode node, MqttClusterEvent event);
}
