package org.kin.mqtt.broker.example;

import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.core.MqttBroker;
import org.kin.mqtt.broker.rule.RuleDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.HttpBridgeActionDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

/**
 * @author huangjianqin
 * @date 2022/12/24
 */
//@Component
public class HttpActionRuleRegister implements ApplicationRunner {
    @Autowired
    private MqttBroker mqttBroker;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String topic = Topics.EXAMPLE;
        RuleDefinition ruleDefinition = RuleDefinition.builder()
                .name("http")
                .desc("message from '" + topic + "' transmit to web")
                .sql("select * from `" + topic + "`")
                .actionDefs(HttpBridgeActionDefinition.builder()
                        .bridgeName(BridgeType.HTTP.getDefaultName())
                        .uri("localhost:10000/mqtt/receive")
                        .build())
                .build();
        mqttBroker.getContext().getRuleManager().addRule(ruleDefinition);
    }
}
