package org.kin.mqtt.broker.event;

import org.kin.framework.reactor.event.EventConsumer;

/**
 * {@link MqttEvent}或{@link org.kin.mqtt.broker.cluster.event.MqttClusterEvent} consumer
 *
 * @author huangjianqin
 * @date 2023/4/24
 */
public interface MqttEventConsumer<E extends MqttEvent> extends EventConsumer<E> {

}
