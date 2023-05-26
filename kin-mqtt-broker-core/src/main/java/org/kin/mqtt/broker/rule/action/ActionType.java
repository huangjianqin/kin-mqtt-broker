package org.kin.mqtt.broker.rule.action;

/**
 * {@link Action}类型
 * @author huangjianqin
 * @date 2023/5/22
 */
public interface ActionType {
    String HTTP_BRIDGE="http_bridge";
    String KAFKA_BRIDGE="kafka_bridge";
    String MQTT_TOPIC="mqtt_topic";
    String RABBIT_MQ_BRIDGE = "rabbitMQ_bridge";
}
