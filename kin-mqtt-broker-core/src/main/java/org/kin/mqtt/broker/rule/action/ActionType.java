package org.kin.mqtt.broker.rule.action;

/**
 * {@link Action}类型
 *
 * @author huangjianqin
 * @date 2023/5/22
 */
public interface ActionType {
    String HTTP_BRIDGE = "httpBridge";
    String KAFKA_BRIDGE = "kafkaBridge";
    String MQTT_TOPIC = "mqttTopic";
    String RABBIT_MQ_BRIDGE = "rabbitMQBridge";
}
