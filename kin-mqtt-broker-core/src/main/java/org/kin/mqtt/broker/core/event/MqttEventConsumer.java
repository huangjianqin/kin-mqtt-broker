package org.kin.mqtt.broker.core.event;

import org.kin.framework.reactor.event.EventConsumer;
import org.kin.mqtt.broker.core.cluster.event.MqttClusterEvent;

/**
 * {@link MqttEvent}或{@link MqttClusterEvent} consumer
 *
 * @author huangjianqin
 * @date 2023/4/24
 */
public interface MqttEventConsumer<E extends MqttEvent> extends EventConsumer<E> {

}
