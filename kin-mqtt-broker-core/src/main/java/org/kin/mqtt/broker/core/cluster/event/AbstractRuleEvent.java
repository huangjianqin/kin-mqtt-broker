package org.kin.mqtt.broker.core.cluster.event;

import org.kin.mqtt.broker.core.event.MqttEvent;

/**
 * 规则相关事件
 * @author huangjianqin
 * @date 2023/5/22
 */
public abstract class AbstractRuleEvent implements MqttEvent {
    /** 规则名 */
    protected String ruleName;

    //setter && getter
    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }
}
