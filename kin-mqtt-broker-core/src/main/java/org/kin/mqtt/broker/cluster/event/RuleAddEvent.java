package org.kin.mqtt.broker.cluster.event;

/**
 * 新增规则事件
 *
 * @author huangjianqin
 * @date 2022/12/20
 */
public class RuleAddEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 8445470898228226326L;

    /** 规则名 */
    private String ruleName;

    public static RuleAddEvent of(String ruleName) {
        RuleAddEvent inst = new RuleAddEvent();
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
