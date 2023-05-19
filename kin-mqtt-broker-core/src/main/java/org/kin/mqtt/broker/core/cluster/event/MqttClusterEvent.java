package org.kin.mqtt.broker.core.cluster.event;

import org.kin.mqtt.broker.core.event.MqttEvent;

import java.io.Serializable;

/**
 * mqtt集群广播事件标志接口
 *
 * @author huangjianqin
 * @date 2022/12/20
 */
public interface MqttClusterEvent extends MqttEvent, Serializable {
}
