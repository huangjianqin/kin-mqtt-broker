package org.kin.mqtt.broker.cluster.event;

/**
 * 移除规则动作事件
 *
 * @author huangjianqin
 * @date 2022/12/20
 */
public class RuleActionRemoveEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 8875413488355878702L;
    /** 规则名 */
    private String ruleName;

    public static RuleActionRemoveEvent of(String ruleName) {
        RuleActionRemoveEvent inst = new RuleActionRemoveEvent();
        inst.ruleName = ruleName;
        return inst;
    }

    //setter && getter

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }
}
