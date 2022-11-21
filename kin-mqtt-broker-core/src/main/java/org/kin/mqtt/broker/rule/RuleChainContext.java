package org.kin.mqtt.broker.rule;

import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;

import java.util.HashMap;
import java.util.Map;

/**
 * 规则链上下文
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class RuleChainContext {
    private final MqttBrokerContext brokerContext;
    private final MqttMessageReplica message;
    private final Map<String, Object> attrs = new HashMap<>();

    public RuleChainContext(MqttBrokerContext brokerContext, MqttMessageReplica message) {
        this.brokerContext = brokerContext;
        this.message = message;

        //将消息属性更新到规则链上下文属性
        attrs.put("clientId", message.getClientId());
        attrs.put("topic", message.getTopic());
        attrs.put("qos", message.getQos());
        attrs.put("retain", message.isRetain());
        attrs.put("payload", message.getPayload());
        attrs.put("timestamp", message.getTimestamp());
        attrs.put("properties", message.getProperties());
    }

    /**
     * 批量更新属性
     *
     * @param otherAttrs 属性
     */
    public void updateAttrs(Map<String, Object> otherAttrs) {
        attrs.putAll(otherAttrs);
    }

    /**
     * 更新单个属性
     *
     * @param key   属性key
     * @param value 属性value
     */
    public void updateAttr(String key, Object value) {
        attrs.put(key, value);
    }

    /**
     * 获取object类型的属性上下文
     *
     * @param key 属性key
     * @param <T> 实际类型
     * @return object
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttr(String key) {
        return (T) attrs.get(key);
    }

    /**
     * 获取boolean类型的属性上下文
     *
     * @param key 属性key
     * @return boolean
     */
    public boolean getBooleanAttr(String key) {
        if (attrs.containsKey(key)) {
            return (boolean) attrs.get(key);
        }

        return false;
    }

    /**
     * 获取byte类型的属性上下文
     *
     * @param key 属性key
     * @return byte
     */
    public byte getByteAttr(String key) {
        if (attrs.containsKey(key)) {
            return (byte) attrs.get(key);
        }

        return 0;
    }

    /**
     * 获取char类型的属性上下文
     *
     * @param key 属性key
     * @return char
     */
    public char getCharAttr(String key) {
        if (attrs.containsKey(key)) {
            return (char) attrs.get(key);
        }

        return 0;
    }

    /**
     * 获取short类型的属性上下文
     *
     * @param key 属性key
     * @return short
     */
    public short getShortAttr(String key) {
        if (attrs.containsKey(key)) {
            return (short) attrs.get(key);
        }

        return 0;
    }

    /**
     * 获取int类型的属性上下文
     *
     * @param key 属性key
     * @return int
     */
    public int getIntAttr(String key) {
        if (attrs.containsKey(key)) {
            return (int) attrs.get(key);
        }

        return 0;
    }

    /**
     * 获取float类型的属性上下文
     *
     * @param key 属性key
     * @return float
     */
    public float getFloatAttr(String key) {
        if (attrs.containsKey(key)) {
            return (float) attrs.get(key);
        }

        return 0.0F;
    }

    /**
     * 获取long类型的属性上下文
     *
     * @param key 属性key
     * @return long
     */
    public long getLongAttr(String key) {
        if (attrs.containsKey(key)) {
            return (long) attrs.get(key);
        }

        return 0L;
    }

    /**
     * 获取double类型的属性上下文
     *
     * @param key 属性key
     * @return double
     */
    public double getDoubleAttr(String key) {
        if (attrs.containsKey(key)) {
            return (double) attrs.get(key);
        }

        return 0.0D;
    }

    //getter
    public MqttBrokerContext getBrokerContext() {
        return brokerContext;
    }

    public MqttMessageReplica getMessage() {
        return message;
    }

    public Map<String, Object> getAttrs() {
        return attrs;
    }
}
