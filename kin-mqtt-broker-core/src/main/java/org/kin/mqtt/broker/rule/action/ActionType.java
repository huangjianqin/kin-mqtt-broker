package org.kin.mqtt.broker.rule.action;

import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.core.MqttBrokerException;
import org.kin.mqtt.broker.rule.action.bridge.definition.HttpBridgeActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.KafkaBridgeActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.MqttTopicActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.RabbitMQBridgeActionDefinition;

import java.util.Objects;

/**
 * {@link Action}类型
 * @author huangjianqin
 * @date 2023/5/22
 */
public enum ActionType {
    HTTP_BRIDGE("http_bridge", HttpBridgeActionDefinition.class),
    KAFKA_BRIDGE("kafka_bridge", KafkaBridgeActionDefinition.class),
    MQTT_TOPIC("mqtt_topic", MqttTopicActionDefinition.class),
    RABBIT_MQ_BRIDGE("rabbitMQ_bridge", RabbitMQBridgeActionDefinition.class),
    ;
    private String name;
    private Class<? extends ActionDefinition> definitionClass;

    private static final ActionType[] VALUES = values();

    /**
     * 根据action类型寻找{@link ActionType}实例
     * @param name  action类型名称
     * @return  {@link ActionType}实例
     */
    public static ActionType findByName(String name){
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("action type name is blank");
        }

        for (ActionType type : VALUES) {
            if (type.name.equalsIgnoreCase(name)) {
                return type;
            }
        }

        throw new MqttBrokerException("can not find any action type which name is " + name);
    }

    /**
     * 根据action definition class寻找{@link ActionType}实例
     * @param definitionClass  action definition class
     * @return  {@link ActionType}实例
     */
    public static ActionType findByDefinition(Class<? extends ActionDefinition> definitionClass){
        if (Objects.isNull(definitionClass)) {
            throw new IllegalArgumentException("action definition class is null");
        }

        for (ActionType type : VALUES) {
            if (type.definitionClass.equals(definitionClass)) {
                return type;
            }
        }

        throw new MqttBrokerException("can not find any action type which definition class is " + definitionClass.getName());
    }

    ActionType(String name, Class<? extends ActionDefinition> definitionClass) {
        this.name = name;
        this.definitionClass = definitionClass;
    }

    public String getName() {
        return name;
    }

    public Class<? extends ActionDefinition> getDefinitionClass() {
        return definitionClass;
    }
}
