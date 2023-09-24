package org.kin.mqtt.broker.rule.action;

import com.google.common.base.Preconditions;
import org.kin.framework.collection.ConfigurationProperties;
import org.kin.framework.collection.MapConfigurationProperties;
import org.kin.framework.utils.StringUtils;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * 动作配置
 * 注意, 子类必须实现{@link  Object#equals(Object)}和{@link Object#hashCode()}, 依赖这两个方法判断动作定义式是否一致, 用于动作移除或更新操作
 *
 * @author huangjianqin
 * @date 2022/12/16
 */
public class ActionConfiguration implements ConfigurationProperties, Serializable {
    private static final long serialVersionUID = -8288965916494361239L;

    /** action type */
    private String type;
    /** action配置 */
    private MapConfigurationProperties props = new MapConfigurationProperties();

    /**
     * 配置检查
     */
    public void check() {
        Preconditions.checkArgument(StringUtils.isNotBlank(type), "action type must not blank");
    }

    @Override
    public void putAll(Map<? extends String, ?> properties) {
        this.props.putAll(properties);
    }

    @Nullable
    @Override
    public Object put(String key, Object obj) {
        return props.put(key, obj);
    }

    @Override
    public boolean contains(String key) {
        return props.contains(key);
    }

    @Nullable
    @Override
    public <T> T get(String key) {
        return props.get(key);
    }

    @Override
    public <T> T get(String key, T defaultValue) {
        return props.get(key, defaultValue);
    }

    @Override
    public boolean getBool(String key, boolean defaultValue) {
        return props.getBool(key, defaultValue);
    }

    @Override
    public byte getByte(String key, byte defaultValue) {
        return props.getByte(key, defaultValue);
    }

    @Override
    public short getShort(String key, short defaultValue) {
        return props.getShort(key, defaultValue);
    }

    @Override
    public int getInt(String key, int defaultValue) {
        return props.getInt(key, defaultValue);
    }

    @Override
    public long getLong(String key, long defaultValue) {
        return props.getLong(key, defaultValue);
    }

    @Override
    public float getFloat(String key, float defaultValue) {
        return props.getFloat(key, defaultValue);
    }

    @Override
    public double getDouble(String key, double defaultValue) {
        return props.getDouble(key, defaultValue);
    }

    @Nullable
    @Override
    public <T> T get(String key, Function<Object, T> func) {
        return props.get(key, func);
    }

    @Override
    public <T> T get(String key, Function<Object, T> func, T defaultValue) {
        return props.get(key, func, defaultValue);
    }

    @Nullable
    @Override
    public <T> T remove(String key) {
        return props.remove(key);
    }

    @Override
    public Map<String, Object> toMap() {
        return props.toMap();
    }

    @Override
    public void clear() {
        props.clear();
    }

    //setter && getter
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public MapConfigurationProperties getProps() {
        return props;
    }

    public void setProps(MapConfigurationProperties props) {
        this.props = props;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionConfiguration)) {
            return false;
        }
        ActionConfiguration that = (ActionConfiguration) o;
        return Objects.equals(type, that.type) && Objects.equals(props, that.props);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, props);
    }

    @Override
    public String toString() {
        return "ActionConfiguration{" +
                "type='" + type + '\'' +
                ", props=" + props +
                '}';
    }
}
