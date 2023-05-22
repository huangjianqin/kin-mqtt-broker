package org.kin.mqtt.broker.core.cluster.event;

/**
 * 新增规则事件
 *
 * @author huangjianqin
 * @date 2022/12/20
 */
public class RuleAddEvent extends AbstractRuleEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 8445470898228226326L;

    public static RuleAddEvent of(String ruleName) {
        RuleAddEvent inst = new RuleAddEvent();
        inst.ruleName = ruleName;
        return inst;
    }
}
