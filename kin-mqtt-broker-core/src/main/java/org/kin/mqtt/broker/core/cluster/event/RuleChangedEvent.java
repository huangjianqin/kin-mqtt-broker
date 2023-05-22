package org.kin.mqtt.broker.core.cluster.event;

/**
 * 规则变化事件
 * @author huangjianqin
 * @date 2023/5/22
 */
public class RuleChangedEvent extends AbstractRuleEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 8445470898228226326L;

    public static RuleChangedEvent of(String ruleName) {
        RuleChangedEvent inst = new RuleChangedEvent();
        inst.ruleName = ruleName;
        return inst;
    }
}
