package org.kin.mqtt.broker.core.cluster.event;

import org.kin.framework.reactor.event.ReactorEventBus;
import org.kin.mqtt.broker.core.MqttBrokerException;
import org.kin.mqtt.broker.core.cluster.BrokerManager;
import org.kin.mqtt.broker.core.cluster.MqttBrokerNode;
import org.kin.mqtt.broker.core.event.MqttEventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * {@link AbstractMqttClusterEvent} consumer
 *
 * @author huangjianqin
 * @date 2023/4/24
 */
public abstract class AbstractMqttClusterEventConsumer<E extends AbstractMqttClusterEvent> implements MqttEventConsumer<E> {
    private static final Logger log = LoggerFactory.getLogger(AbstractMqttClusterEventConsumer.class);

    /** 集群节点管理 */
    protected final BrokerManager brokerManager;

    protected AbstractMqttClusterEventConsumer(BrokerManager brokerManager) {
        this.brokerManager = brokerManager;
    }

    @Override
    public final void consume(ReactorEventBus eventBus, E event) {
        String id = event.getId();
        String address = event.getAddress();
        MqttBrokerNode node = brokerManager.getNodeById(id);
        if (Objects.isNull(node)) {
            throw new MqttBrokerException(String.format("receive cluster event from node '%s'(%s) which is not in cluster", id, address));
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
    protected abstract void consume(ReactorEventBus eventBus, MqttBrokerNode node, E event);
}
