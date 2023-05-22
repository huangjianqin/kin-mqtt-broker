package org.kin.mqtt.broker.rule.action;

import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.utils.ClassUtils;
import org.kin.mqtt.broker.rule.action.bridge.HttpBridgeAction;
import org.kin.mqtt.broker.rule.action.bridge.KafkaBridgeAction;
import org.kin.mqtt.broker.rule.action.bridge.MqttTopicAction;
import org.kin.mqtt.broker.rule.action.bridge.RabbitMQBridgeAction;
import org.kin.mqtt.broker.rule.action.bridge.definition.HttpBridgeActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.KafkaBridgeActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.MqttTopicActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.RabbitMQBridgeActionDefinition;

import java.util.*;

/**
 * action工具类
 * @author huangjianqin
 * @date 2022/12/16
 */
public class Actions {
    private Actions() {
    }

    private static Map<Class<? extends ActionDefinition>, ActionFactory<? extends ActionDefinition, ? extends Action>> ACTIONS = new NonBlockingHashMap<>();

    static {
        registerAction(HttpBridgeActionDefinition.class, (ActionFactory<HttpBridgeActionDefinition, HttpBridgeAction>) HttpBridgeAction::new);
        registerAction(KafkaBridgeActionDefinition.class, (ActionFactory<KafkaBridgeActionDefinition, KafkaBridgeAction>) KafkaBridgeAction::new);
        registerAction(MqttTopicActionDefinition.class, (ActionFactory<MqttTopicActionDefinition, MqttTopicAction>) MqttTopicAction::new);
        registerAction(RabbitMQBridgeActionDefinition.class, (ActionFactory<RabbitMQBridgeActionDefinition, RabbitMQBridgeAction>) RabbitMQBridgeAction::new);
    }

    /**
     * 创建{@link Action}实例
     *
     * @return {@link Action}实例
     */
    @SuppressWarnings("unchecked")
    public static Action createAction(ActionDefinition actionDefinition) {
        Class<? extends ActionDefinition> claxx = actionDefinition.getClass();
        ActionFactory<ActionDefinition, Action> factory = (ActionFactory<ActionDefinition, Action>) ACTIONS.get(claxx);
        if (Objects.isNull(factory)) {
            throw new IllegalStateException(String.format("is not support action definition '%s'", claxx.getName()));
        }

        return factory.create(actionDefinition);
    }

    /**
     * 注册{@link Action}实现
     * 通过{@link  ActionFactory}实现类泛型获取{@link ActionDefinition}实现类信息, 注意, 此处{@link  ActionFactory}实现类不能是匿名内部类和lambda
     * 自定义{@link Action}实现, 可通过该方法注册自定义{@link Action}构建逻辑
     *
     * @param factories {@link Action}实现构造逻辑
     */
    public static void registerActionsByGeneric(ActionFactory<? extends ActionDefinition, ? extends Action>... factories) {
        registerActionsByGeneric(Arrays.asList(factories));
    }

    /**
     * 注册{@link Action}实现
     * 通过{@link  ActionFactory}实现类泛型获取{@link ActionDefinition}实现类信息, 注意, 此处{@link  ActionFactory}实现类不能是匿名内部类和lambda
     * 自定义{@link Action}实现, 可通过该方法注册自定义{@link Action}构建逻辑
     * TODO README
     *
     * @param factories {@link Action}实现构造逻辑
     */
    @SuppressWarnings("unchecked")
    public static void registerActionsByGeneric(Collection<ActionFactory<? extends ActionDefinition, ? extends Action>> factories) {
        Map<Class<? extends ActionDefinition>, ActionFactory<? extends ActionDefinition, ? extends Action>> map = new HashMap<>();
        for (ActionFactory<? extends ActionDefinition, ? extends Action> factory : factories) {
            List<Class<?>> genericTypes = ClassUtils.getSuperInterfacesGenericRawTypes(ActionFactory.class, factory.getClass());
            Class<? extends ActionDefinition> adClass = (Class<? extends ActionDefinition>) genericTypes.get(0);

            if (ACTIONS.containsKey(adClass)) {
                throw new IllegalStateException(String.format("action with '%s' definition has registered", adClass.getName()));
            }
            map.put(adClass, factory);
        }

        ACTIONS.putAll(map);
    }

    /**
     * 注册{@link Action}实现
     * 自定义{@link Action}实现, 可通过该方法注册自定义{@link Action}构建逻辑
     *
     * @param adClass action定义class
     * @param factory {@link Action}实现构造逻辑
     */
    public static void registerAction(Class<? extends ActionDefinition> adClass, ActionFactory<? extends ActionDefinition, ? extends Action> factory) {
        if (ACTIONS.containsKey(adClass)) {
            throw new IllegalStateException(String.format("action with '%s' definition has registered", adClass.getName()));
        }
        ACTIONS.put(adClass, factory);
    }
}
