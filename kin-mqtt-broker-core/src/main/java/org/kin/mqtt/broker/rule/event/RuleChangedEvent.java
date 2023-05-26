package org.kin.mqtt.broker.rule.event;

import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.core.cluster.event.MqttClusterEvent;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 规则变化事件
 *
 * @author huangjianqin
 * @date 2023/5/22
 */
public class RuleChangedEvent extends AbstractRuleEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 8445470898228226326L;

    public static RuleChangedEvent of(String... ruleNames) {
        return of(Arrays.asList(ruleNames));
    }

    public static RuleChangedEvent of(List<String> ruleNames) {
        RuleChangedEvent inst = new RuleChangedEvent();
        inst.ruleNames = CollectionUtils.isNonEmpty(ruleNames) ? ruleNames : Collections.emptyList();
        return inst;
    }
}
