package org.kin.mqtt.broker.rule;

import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 规则执行入口
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class RuleChainExecutor {
    /** 规则链 */
    private final RuleChainManager ruleChainManager;

    public RuleChainExecutor(RuleChainManager ruleChainManager) {
        this.ruleChainManager = ruleChainManager;
    }

    /**
     * 规则执行入口
     *
     * @param brokerContext mqtt broker context
     * @param message       mqtt publish消息副本
     */
    public Mono<Void> execute(MqttBrokerContext brokerContext, MqttMessageReplica message) {
        return Flux.fromIterable(ruleChainManager.getRuleChains())
                .flatMap(ruleChain -> ruleChain.execute(new RuleChainContext(brokerContext, message)))
                .then();
    }
}
