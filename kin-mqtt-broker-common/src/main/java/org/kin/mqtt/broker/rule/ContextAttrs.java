package org.kin.mqtt.broker.rule;

import java.util.HashMap;
import java.util.Map;

/**
 * 规则链上下文属性
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public final class ContextAttrs extends HashMap<String, Object> {
    private static final long serialVersionUID = -6641783935850596222L;

    /**
     * 批量更新属性
     *
     * @param otherAttrs 属性
     */
    public void updateAttrs(Map<String, Object> otherAttrs) {
        putAll(otherAttrs);
    }

    /**
     * 更新单个属性
     *
     * @param key   属性key
     * @param value 属性value
     */
    public void updateAttr(String key, Object value) {
        put(key, value);
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
        if (!containsKey(key)) {
            throw new IllegalArgumentException(String.format("attr '%s' is not exists", key));
        } else {
            return (T) get(key);
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
        if (!containsKey(key)) {
            return defaultValue;
        } else {
            return (T) get(key);
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
    public <T> T rmAttr(String key) {
        if (!containsKey(key)) {
            throw new IllegalArgumentException(String.format("attr '%s' is not exists", key));
        } else {
            return (T) remove(key);
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
    public <T> T rmAttrOrDefault(String key, T defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        } else {
            return (T) remove(key);
        }
    }

    /**
     * 获取boolean类型的属性上下文
     *
     * @param key 属性key
     * @return boolean
     */
    public boolean getBooleanAttr(String key) {
        if (containsKey(key)) {
            return (boolean) get(key);
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
        if (containsKey(key)) {
            return (byte) get(key);
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
        if (containsKey(key)) {
            return (char) get(key);
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
        if (containsKey(key)) {
            return (short) get(key);
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
        if (containsKey(key)) {
            return (int) get(key);
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
        if (containsKey(key)) {
            return (float) get(key);
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
        if (containsKey(key)) {
            return (long) get(key);
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
        if (containsKey(key)) {
            return (double) get(key);
        }

        return 0.0D;
    }
}
