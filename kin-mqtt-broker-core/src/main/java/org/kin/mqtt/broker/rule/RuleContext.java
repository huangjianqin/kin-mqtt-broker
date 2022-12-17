package org.kin.mqtt.broker.rule;

import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;

/**
 * 规则链上下文
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class RuleContext {
    /** mqtt broker context */
    private final MqttBrokerContext brokerContext;
    /** publish message副本 */
    private final MqttMessageReplica message;
    /** 规则上下文属性 */
    private final ContextAttrs attrs = new ContextAttrs();

    public RuleContext(MqttBrokerContext brokerContext, MqttMessageReplica message) {
        this.brokerContext = brokerContext;
        this.message = message;
    }

    //getter
    public MqttBrokerContext getBrokerContext() {
        return brokerContext;
    }

    public ContextAttrs getAttrs() {
        return attrs;
    }

    public MqttMessageReplica getMessage() {
        return message;
    }
}
