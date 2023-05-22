package org.kin.mqtt.broker.core.cluster.event;

/**
 * 规则移除事件
 *
 * @author huangjianqin
 * @date 2022/12/20
 */
public class RuleRemoveEvent extends AbstractRuleEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 3202072813783134098L;

    public static RuleRemoveEvent of(String ruleName) {
        RuleRemoveEvent inst = new RuleRemoveEvent();
        inst.ruleName = ruleName;
        return inst;
    }
}
