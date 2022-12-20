package org.kin.mqtt.broker.cluster.event;

/**
 * 新增规则动作事件
 *
 * @author huangjianqin
 * @date 2022/12/20
 */
public class RuleActionAddEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 5510626091708527604L;
    /** 规则名 */
    private String ruleName;

    public static RuleActionAddEvent of(String ruleName) {
        RuleActionAddEvent inst = new RuleActionAddEvent();
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
