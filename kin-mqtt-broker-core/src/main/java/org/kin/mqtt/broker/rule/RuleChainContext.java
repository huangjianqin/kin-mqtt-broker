package org.kin.mqtt.broker.rule;

import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;

/**
 * 规则链上下文
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class RuleChainContext {
    private final MqttBrokerContext brokerContext;
    private final MqttMessageReplica message;
    private final ContextAttrs attrs = new ContextAttrs();

    public RuleChainContext(MqttBrokerContext brokerContext, MqttMessageReplica message) {
        this.brokerContext = brokerContext;
        this.message = message;

        //将消息属性更新到规则链上下文属性
        attrs.updateAttr(RuleChainAttrNames.MQTT_CLIENT_ID, message.getClientId());
        attrs.updateAttr(RuleChainAttrNames.MQTT_MSG_TOPIC, message.getTopic());
        attrs.updateAttr(RuleChainAttrNames.MQTT_MSG_QOS, message.getQos());
        attrs.updateAttr(RuleChainAttrNames.MQTT_MSG_RETAIN, message.isRetain());
        attrs.updateAttr(RuleChainAttrNames.MQTT_MSG_PAYLOAD, message.getPayload());
        attrs.updateAttr(RuleChainAttrNames.MQTT_MSG_TIMESTAMP, message.getTimestamp());
        attrs.updateAttr(RuleChainAttrNames.MQTT_MSG_PROPERTIES, message.getProperties());
    }

    //getter
    public MqttBrokerContext getBrokerContext() {
        return brokerContext;
    }

    public MqttMessageReplica getMessage() {
        return message;
    }

    public ContextAttrs getAttrs() {
        return attrs;
    }
}
