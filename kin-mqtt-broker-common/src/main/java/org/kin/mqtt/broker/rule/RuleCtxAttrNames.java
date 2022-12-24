package org.kin.mqtt.broker.rule;

/**
 * 规则上下文内置属性名
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public interface RuleCtxAttrNames {
    /** mqtt client id */
    String MQTT_CLIENT_ID = "MQTT_CLIENT_ID";
    /** mqtt publish message topic */
    String MQTT_MSG_TOPIC = "MQTT_MSG_TOPIC";
    /** mqtt publish message qos */
    String MQTT_MSG_QOS = "MQTT_MSG_QOS";
    /** mqtt publish message retain */
    String MQTT_MSG_RETAIN = "MQTT_MSG_RETAIN";
    /** mqtt publish message payload */
    String MQTT_MSG_PAYLOAD = "MQTT_MSG_PAYLOAD";
    /** mqtt publish message timestamp */
    String MQTT_MSG_TIMESTAMP = "MQTT_MSG_TIMESTAMP";
    /** mqtt publish message properties */
    String MQTT_MSG_PROPERTIES = "MQTT_MSG_PROPERTIES";
}
