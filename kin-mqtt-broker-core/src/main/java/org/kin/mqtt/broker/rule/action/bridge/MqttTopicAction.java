package org.kin.mqtt.broker.rule.action.bridge;

import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.VirtualMqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.rule.RuleContext;
import org.kin.mqtt.broker.rule.action.Action;
import org.kin.mqtt.broker.rule.action.ActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.MqttTopicActionDefinition;
import reactor.core.publisher.Mono;

/**
 * 转发到指定mqtt topic
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public class MqttTopicAction implements Action {
    /** mqtt topic转发topic定义 */
    private final MqttTopicActionDefinition definition;

    public MqttTopicAction(MqttTopicActionDefinition definition) {
        this.definition = definition;
    }

    @Override
    public Mono<Void> start(RuleContext context) {
        MqttBrokerContext brokerContext = context.getBrokerContext();
        MqttMessageReplica replica = context.getMessage();
        //script即真正topic
        //交给mqtt消息handler处理
        return Mono.fromRunnable(() -> brokerContext.getDispatcher().dispatch(
                MqttMessageWrapper.common(MqttMessageUtils.createPublish(replica, definition.getTopic())),
                new VirtualMqttSession(brokerContext, replica.getClientId()),
                brokerContext));
    }

    @Override
    public ActionDefinition definition() {
        return definition;
    }
}
