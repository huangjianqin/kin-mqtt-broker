package org.kin.mqtt.broker.cluster.event;

import org.kin.mqtt.broker.event.MqttEventConsumer;

/**
 * {@link MqttClusterEvent} consumer
 *
 * @author huangjianqin
 * @date 2023/4/24
 */
public interface MqttClusterEventConsumer<E extends MqttClusterEvent> extends MqttEventConsumer<E> {
}
