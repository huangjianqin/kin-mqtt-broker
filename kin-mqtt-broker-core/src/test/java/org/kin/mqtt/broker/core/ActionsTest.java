package org.kin.mqtt.broker.core;

import org.kin.mqtt.broker.rule.action.Actions;
import org.kin.mqtt.broker.rule.action.bridge.definition.HttpActionDefinition;

/**
 * @author huangjianqin
 * @date 2022/12/17
 */
public class ActionsTest {
    public static void main(String[] args) {
        Actions.createAction(HttpActionDefinition.builder().build());
    }
}
