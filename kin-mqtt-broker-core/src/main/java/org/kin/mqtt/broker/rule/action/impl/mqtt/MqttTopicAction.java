package org.kin.mqtt.broker.rule.action.impl.mqtt;

import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.rule.RuleContext;
import org.kin.mqtt.broker.rule.action.AbstractAction;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;
import reactor.core.publisher.Mono;

import static org.kin.mqtt.broker.rule.action.impl.mqtt.MqttTopicActionConstants.QOS_KEY;
import static org.kin.mqtt.broker.rule.action.impl.mqtt.MqttTopicActionConstants.TOPIC_KEY;

/**
 * 转发到指定mqtt topic
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public class MqttTopicAction extends AbstractAction {
    public MqttTopicAction(ActionConfiguration config) {
        super(config);
    }

    @Override
    public Mono<Void> execute(RuleContext context) {
        MqttBrokerContext brokerContext = context.getBrokerContext();
        MqttMessageReplica replica = context.getMessage();
        //script即真正topic
        //交给mqtt消息handler处理
        return brokerContext.getDispatcher()
                .dispatch(MqttMessageContext.common(MqttMessageHelper.createPublish(replica, config.get(TOPIC_KEY), config.get(QOS_KEY)),
                                brokerContext.getBrokerId(), replica.getClientId()),
                        null,
                        brokerContext);
    }
}
