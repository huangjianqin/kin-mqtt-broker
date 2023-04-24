package org.kin.mqtt.broker.event;

import org.kin.framework.reactor.event.EventConsumer;

/**
 * {@link MqttEvent} consumer
 *
 * @author huangjianqin
 * @date 2023/4/24
 */
public abstract interface MqttEventConsumer<E extends MqttEvent> extends EventConsumer<E> {

}
