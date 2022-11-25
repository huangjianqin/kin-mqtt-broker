package org.kin.mqtt.broker.rule.impl;

import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.VirtualMqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.rule.ConditionLessRuleNode;
import org.kin.mqtt.broker.rule.RuleChainContext;
import org.kin.mqtt.broker.rule.RuleDefinition;
import org.kin.mqtt.broker.rule.RuleNode;
import reactor.core.publisher.Mono;

/**
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class TopicRule extends ConditionLessRuleNode {
    public TopicRule(RuleDefinition definition) {
        super(definition);
    }

    public TopicRule(RuleDefinition definition, RuleNode next) {
        super(definition, next);
    }

    @Override
    protected Mono<Void> execute0(RuleChainContext context) {
        MqttBrokerContext brokerContext = context.getBrokerContext();
        MqttMessageReplica replica = context.getMessage();
        //script即真正topic
        //交给mqtt消息handler处理
        return Mono.fromRunnable(() -> brokerContext.getDispatcher().dispatch(
                MqttMessageWrapper.common(MqttMessageUtils.createPublish(replica, definition.getScript())),
                new VirtualMqttChannel(brokerContext, replica.getClientId()),
                brokerContext));
    }
}
