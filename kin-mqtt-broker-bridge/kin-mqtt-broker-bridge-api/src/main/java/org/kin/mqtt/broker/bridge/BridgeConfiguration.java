package org.kin.mqtt.broker.bridge;

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
 * bridge配置
 *
 * @author huangjianqin
 * @date 2023/9/23
 */
public class BridgeConfiguration implements ConfigurationProperties, Serializable {
    private static final long serialVersionUID = -4688448786070328116L;
    /** bridge name */
    private String name;
    /** bridge描述 */
    private String desc;
    /** bridge type */
    private String type;
    /** bridge配置 */
    private MapConfigurationProperties props = new MapConfigurationProperties();

    /**
     * 配置检查
     */
    public void check() {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "bridge name must not blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(type), "bridge type must not blank");
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
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

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
        if (!(o instanceof BridgeConfiguration)) {
            return false;
        }
        BridgeConfiguration that = (BridgeConfiguration) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "BridgeConfiguration{" +
                "name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", props=" + props +
                '}';
    }

    //---------------------------------------------------------------------------------------------------------------------
    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final BridgeConfiguration configuration = new BridgeConfiguration();

        public Builder name(String name) {
            configuration.name = name;
            return this;
        }

        public Builder desc(String desc) {
            configuration.desc = desc;
            return this;
        }

        /**
         * @see BridgeType
         */
        public Builder type(String type) {
            configuration.type = type;
            return this;
        }

        public Builder properties(Map<String, Object> props) {
            configuration.props.putAll(props);
            return this;
        }

        public Builder property(String key, Object value) {
            configuration.props.put(key, value);
            return this;
        }

        public BridgeConfiguration build() {
            return configuration;
        }
    }
}
