package org.kin.mqtt.broker.rule;

/**
 * 规则上下文内置属性名
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public interface RuleCtxAttrNames {
    /** mqtt client id */
    String MQTT_CLIENT_ID = "clientId";
    /** mqtt publish message topic */
    String MQTT_MSG_TOPIC = "topic";
    /** mqtt publish message qos */
    String MQTT_MSG_QOS = "qos";
    /** mqtt publish message retain */
    String MQTT_MSG_RETAIN = "retain";
    /** mqtt publish message payload */
    String MQTT_MSG_PAYLOAD = "payload";
    /** mqtt publish message timestamp */
    String MQTT_MSG_TIMESTAMP = "timestamp";
    /** mqtt publish message properties */
    String MQTT_MSG_PROPERTIES = "properties";
}
