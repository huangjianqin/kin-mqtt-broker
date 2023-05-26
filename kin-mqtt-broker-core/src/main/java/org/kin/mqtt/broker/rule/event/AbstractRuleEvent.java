package org.kin.mqtt.broker.rule.event;

import org.kin.mqtt.broker.core.event.MqttEvent;

import java.util.List;

/**
 * 规则相关事件
 * @author huangjianqin
 * @date 2023/5/22
 */
public abstract class AbstractRuleEvent implements MqttEvent {
    /** 规则名 */
    protected List<String> ruleNames;

    //setter && getter
    public List<String> getRuleNames() {
        return ruleNames;
    }

    public void setRuleNames(List<String> ruleNames) {
        this.ruleNames = ruleNames;
    }
}
