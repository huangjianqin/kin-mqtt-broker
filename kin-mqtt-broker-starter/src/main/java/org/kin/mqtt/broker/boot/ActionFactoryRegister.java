package org.kin.mqtt.broker.boot;

import org.kin.mqtt.broker.rule.action.ActionFactory;
import org.kin.mqtt.broker.rule.action.Actions;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 自动注册{@link org.kin.mqtt.broker.rule.action.ActionFactory}
 * @author huangjianqin
 * @date 2023/5/26
 */
@Component
public class ActionFactoryRegister implements ApplicationListener<ContextRefreshedEvent> {
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        ApplicationContext context = event.getApplicationContext();
        Map<String, ActionFactory> actionFactoryMap = context.getBeansOfType(ActionFactory.class);
        for (ActionFactory actionFactory : actionFactoryMap.values()) {
            Actions.registerAction(actionFactory);
        }
    }
}
