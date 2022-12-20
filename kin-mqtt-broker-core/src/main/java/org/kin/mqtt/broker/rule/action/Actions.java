package org.kin.mqtt.broker.rule.action;

import org.kin.framework.collection.CopyOnWriteMap;
import org.kin.framework.utils.ClassUtils;
import org.kin.mqtt.broker.rule.action.bridge.HttpBridgeAction;
import org.kin.mqtt.broker.rule.action.bridge.KafkaBridgeAction;
import org.kin.mqtt.broker.rule.action.bridge.MqttTopicAction;
import org.kin.mqtt.broker.rule.action.bridge.RabbitMQBridgeAction;
import org.kin.mqtt.broker.rule.action.bridge.definition.HttpActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.KafkaActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.MqttTopicActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.RabbitMQActionDefinition;

import java.util.*;

/**
 * @author huangjianqin
 * @date 2022/12/16
 */
public class Actions {
    private Actions() {
    }

    private static Map<Class<? extends ActionDefinition>, ActionFactory<? extends ActionDefinition, ? extends Action>> ACTIONS = new CopyOnWriteMap<>();

    static {
        registerActions(new ActionFactory<HttpActionDefinition, HttpBridgeAction>() {
                            @Override
                            public HttpBridgeAction create(HttpActionDefinition ad) {
                                return new HttpBridgeAction(ad);
                            }
                        },
                new ActionFactory<KafkaActionDefinition, KafkaBridgeAction>() {
                    @Override
                    public KafkaBridgeAction create(KafkaActionDefinition ad) {
                        return new KafkaBridgeAction(ad);
                    }
                },
                new ActionFactory<MqttTopicActionDefinition, MqttTopicAction>() {
                    @Override
                    public MqttTopicAction create(MqttTopicActionDefinition ad) {
                        return new MqttTopicAction(ad);
                    }
                },
                new ActionFactory<RabbitMQActionDefinition, RabbitMQBridgeAction>() {
                    @Override
                    public RabbitMQBridgeAction create(RabbitMQActionDefinition ad) {
                        return new RabbitMQBridgeAction(ad);
                    }
                });
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
     *
     * @param factories {@link Action}实现构造逻辑
     */
    public static void registerActions(ActionFactory<? extends ActionDefinition, ? extends Action>... factories) {
        registerActions(Arrays.asList(factories));
    }

    /**
     * 注册{@link Action}实现
     *
     * @param factories {@link Action}实现构造逻辑
     */
    @SuppressWarnings("unchecked")
    public static void registerActions(Collection<ActionFactory<? extends ActionDefinition, ? extends Action>> factories) {
        Map<Class<? extends ActionDefinition>, ActionFactory<? extends ActionDefinition, ? extends Action>> map = new HashMap<>();
        for (ActionFactory<? extends ActionDefinition, ? extends Action> factory : factories) {
            List<Class<?>> genericTypes = ClassUtils.getSuperInterfacesGenericRawTypes(ActionFactory.class, factory.getClass());
            map.put((Class<? extends ActionDefinition>) genericTypes.get(0), factory);
        }

        ACTIONS.putAll(map);
    }
}
