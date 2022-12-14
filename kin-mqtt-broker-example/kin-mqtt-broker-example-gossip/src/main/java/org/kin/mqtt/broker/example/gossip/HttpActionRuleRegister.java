package org.kin.mqtt.broker.example.gossip;

import org.kin.mqtt.broker.bridge.Bridge;
import org.kin.mqtt.broker.core.MqttBroker;
import org.kin.mqtt.broker.rule.RuleDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.HttpActionDefinition;
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
        String topic = "MQTT Examples";
        RuleDefinition ruleDefinition = RuleDefinition.builder()
                .name("http")
                .desc("message from '" + topic + "' transmit to web")
                .sql("select * from `" + topic + "`")
                .actionDefs(HttpActionDefinition.builder()
                        .bridgeName(Bridge.DEFAULT_NAME)
                        .uri("localhost:10000/mqtt/receive")
                        .build())
                .build();
        mqttBroker.getContext().getRuleManager().addRule(ruleDefinition);
    }
}
