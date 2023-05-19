package org.kin.mqtt.broker.core.cluster.event;

/**
 * 规则移除事件
 *
 * @author huangjianqin
 * @date 2022/12/20
 */
public class RuleRemoveEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 3202072813783134098L;
    /** 规则名 */
    private String ruleName;

    public static RuleRemoveEvent of(String ruleName) {
        RuleRemoveEvent inst = new RuleRemoveEvent();
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
