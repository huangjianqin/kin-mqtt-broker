package org.kin.mqtt.broker.rule.event;

import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.core.cluster.event.MqttClusterEvent;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 新增规则事件
 *
 * @author huangjianqin
 * @date 2022/12/20
 */
public class RuleAddEvent extends AbstractRuleEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 8445470898228226326L;

    public static RuleAddEvent of(String... ruleNames) {
        return of(Arrays.asList(ruleNames));
    }

    public static RuleAddEvent of(List<String> ruleNames) {
        RuleAddEvent inst = new RuleAddEvent();
        inst.ruleNames = CollectionUtils.isNonEmpty(ruleNames) ? ruleNames : Collections.emptyList();
        return inst;
    }
}
