package org.kin.mqtt.broker.rule.action;

import org.kin.framework.collection.CopyOnWriteMap;
import org.kin.framework.utils.ClassUtils;
import org.kin.mqtt.broker.core.Type;
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
 * {@link Action}工具类
 * @author huangjianqin
 * @date 2022/12/16
 */
public class Actions {
    private Actions() {
    }

    private static Map<Class<? extends ActionDefinition>, ActionFactory<? extends ActionDefinition, ? extends Action>> ACTIONS = new CopyOnWriteMap<>();
    /**
     * 关联action type name和{@link ActionDefinition}
     * key -> action type name, value -> {@link ActionDefinition}
     */
    private static final Map<String, Class<? extends ActionDefinition>> NAME_2_DEFINITION = new CopyOnWriteMap<>();

    static {
        registerActionFactory(HttpBridgeActionDefinition.class, (ActionFactory<HttpBridgeActionDefinition, HttpBridgeAction>) HttpBridgeAction::new);
        registerActionFactory(KafkaBridgeActionDefinition.class, (ActionFactory<KafkaBridgeActionDefinition, KafkaBridgeAction>) KafkaBridgeAction::new);
        registerActionFactory(MqttTopicActionDefinition.class, (ActionFactory<MqttTopicActionDefinition, MqttTopicAction>) MqttTopicAction::new);
        registerActionFactory(RabbitMQBridgeActionDefinition.class, (ActionFactory<RabbitMQBridgeActionDefinition, RabbitMQBridgeAction>) RabbitMQBridgeAction::new);
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
            throw new IllegalStateException(String.format("action definition '%s' is not support", claxx.getName()));
        }

        return factory.create(actionDefinition);
    }

    /**
     * 根据action type获取{@link ActionDefinition}类型
     * @return {@link ActionDefinition}类型
     */
    public static Class<? extends ActionDefinition> getDefinitionClassByName(String typeName){
        Class<? extends ActionDefinition> adClass = NAME_2_DEFINITION.get(typeName.toLowerCase());
        if (adClass == null) {
            throw new IllegalArgumentException(String.format("can not find action definition class associated with " + typeName));
        }
        return adClass;
    }

    /**
     * 注册{@link Action}实现
     * 通过{@link  ActionFactory}实现类泛型获取{@link ActionDefinition}实现类信息, 注意, 此处{@link  ActionFactory}实现类不能是匿名内部类和lambda
     * 自定义{@link Action}实现, 可通过该方法注册自定义{@link Action}构建逻辑
     *
     * @param factories {@link Action}实现构造逻辑
     */
    @SuppressWarnings("rawtypes")
    public static void registerActionFactories(ActionFactory... factories) {
        registerActionFactories(Arrays.asList(factories));
    }

    /**
     * 注册{@link Action}实现
     * 通过{@link  ActionFactory}实现类泛型获取{@link ActionDefinition}实现类信息, 注意, 此处{@link  ActionFactory}实现类不能是匿名内部类和lambda
     * 自定义{@link Action}实现, 可通过该方法注册自定义{@link Action}构建逻辑
     *
     * @param factories {@link Action}实现构造逻辑
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void registerActionFactories(Collection<ActionFactory> factories) {
        Map<Class<? extends ActionDefinition>, ActionFactory<? extends ActionDefinition, ? extends Action>> definition2Factory = new HashMap<>();
        Map<String, Class<? extends ActionDefinition>> name2definition = new HashMap<>();
        for (ActionFactory<? extends ActionDefinition, ? extends Action> factory : factories) {
            List<Class<?>> genericTypes = ClassUtils.getSuperInterfacesGenericRawTypes(ActionFactory.class, factory.getClass());
            Class<? extends ActionDefinition> adClass = (Class<? extends ActionDefinition>) genericTypes.get(0);

            if (ACTIONS.containsKey(adClass)) {
                throw new IllegalStateException(String.format("action with '%s' definition has registered", adClass.getName()));
            }

            Type typeAnno = adClass.getAnnotation(Type.class);
            if (typeAnno == null) {
                throw new IllegalArgumentException("can not find Type annotation on action definition class");
            }

            name2definition.put(typeAnno.value().toLowerCase(), adClass);
            definition2Factory.put(adClass, factory);
        }

        NAME_2_DEFINITION.putAll(name2definition);
        ACTIONS.putAll(definition2Factory);
    }

    /**
     * 注册{@link Action}实现
     * 自定义{@link Action}实现, 可通过该方法注册自定义{@link Action}构建逻辑
     *
     * @param adClass action定义class
     * @param factory {@link Action}实现构造逻辑
     */
    public static void registerActionFactory(Class<? extends ActionDefinition> adClass, ActionFactory<? extends ActionDefinition, ? extends Action> factory) {
        if (ACTIONS.containsKey(adClass)) {
            throw new IllegalStateException(String.format("action with '%s' definition has registered", adClass.getName()));
        }

        Type typeAnno = adClass.getAnnotation(Type.class);
        if (typeAnno == null) {
            throw new IllegalArgumentException("can not find Type annotation on action definition class");
        }

        NAME_2_DEFINITION.put(typeAnno.value().toLowerCase(), adClass);
        ACTIONS.put(adClass, factory);
    }
}
