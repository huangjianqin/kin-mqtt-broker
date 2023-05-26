package org.kin.mqtt.broker.rule.event;

import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.core.cluster.event.MqttClusterEvent;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 规则移除事件
 *
 * @author huangjianqin
 * @date 2022/12/20
 */
public class RuleRemoveEvent extends AbstractRuleEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 3202072813783134098L;

    public static RuleRemoveEvent of(String... ruleNames) {
        return of(Arrays.asList(ruleNames));
    }

    public static RuleRemoveEvent of(List<String> ruleNames) {
        RuleRemoveEvent inst = new RuleRemoveEvent();
        inst.ruleNames = CollectionUtils.isNonEmpty(ruleNames) ? ruleNames : Collections.emptyList();
        return inst;
    }
}
