package org.kin.mqtt.broker.rule;

import java.util.HashMap;
import java.util.Map;

/**
 * 规则链上下文属性
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public class ContextAttrs {
    private static final long serialVersionUID = -6641783935850596222L;

    /** 属性 */
    private final HashMap<String, Object> attrs = new HashMap<>();

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
     * 获取属性值, 如果不存在, 或者类型不对, 则抛异常
     *
     * @param key 属性名
     * @param <T> 属性值类型
     * @return 属性值
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttr(String key) {
        if (!attrs.containsKey(key)) {
            throw new IllegalArgumentException(String.format("attr '%s' is not exists", key));
        } else {
            return (T) attrs.get(key);
        }
    }

    /**
     * 获取属性值, 如果不存在, 或者类型不对, 则返回默认值
     *
     * @param key          属性名
     * @param defaultValue 默认值
     * @param <T>          属性值类型
     * @return 属性值
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttrOrDefault(String key, T defaultValue) {
        if (!attrs.containsKey(key)) {
            return defaultValue;
        } else {
            return (T) attrs.get(key);
        }
    }

    /**
     * 移除并返回属性值, 如果不存在, 或者类型不对, 则抛异常
     *
     * @param key 属性名
     * @param <T> 属性值类型
     * @return 属性值
     */
    @SuppressWarnings("unchecked")
    public <T> T removeAttr(String key) {
        if (!attrs.containsKey(key)) {
            throw new IllegalArgumentException(String.format("attr '%s' is not exists", key));
        } else {
            return (T) attrs.remove(key);
        }
    }

    /**
     * 移除并返回属性值, 如果不存在, 或者类型不对, 则返回默认值
     *
     * @param key          属性名
     * @param defaultValue 默认值
     * @param <T>          属性值类型
     * @return 属性值
     */
    @SuppressWarnings("unchecked")
    public <T> T removeAttrOrDefault(String key, T defaultValue) {
        if (!attrs.containsKey(key)) {
            return defaultValue;
        } else {
            return (T) attrs.remove(key);
        }
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
}
